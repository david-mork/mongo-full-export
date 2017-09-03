#!/usr/bin/env node --harmony
const co = require('co');
const prompt = require('co-prompt');
const program = require('commander');
const RSVP = require('rsvp');
const fs = require('fs');
const { exec } = require('child_process');

let mapCollections;
mapCollections = function (val) {
	return val.trim().split(',');
}

let options = {};
let processed = [];
let failed = [];

program
	.arguments('<action>', 'Action to do')
	.option('-I, --import', 'Import')
	.option('-E, --export', 'Export')
	.option('-d, --database <database>', 'Database name')
	.option('-h, --host <host>', 'Host. Default localhost')
	.option('-p, --port <port>', 'Port. Default 27017')
	.option('-U, --user <user>', 'Username to log in to database')
	.option('-P, --password <password>', 'Password of given user to log in to database')
	.option('-c, --collections <collections>', 'List of collections separated by comma \',\'. (All will be used if there is no specified)', mapCollections)
	.option('-o, --output <output>', 'Output directory (exporting)')
	.option('-f, --from <from>', 'Directory where the collections are located (importing)')
	.action(function (action) {

		co(function *() {
			options.action = action.toLowerCase();

			if (options.action != 'import' && options.action != 'export') {
				console.info("Unrecognized action: '%s'", options.action);
				process.exit(1);
			}

			options.database = program.database || (yield prompt('Database name: '));
			options.host = program.host || (yield prompt('Host (localhost): '));
			options.port = program.port || (yield prompt('Port (27017): '));
			options.user = program.user || (yield prompt('DB login username: '));
			options.password = options.user ? (program.password || (yield prompt.password('DB login password: '))) : '';
			options.collections = options.action == 'export' ? (program.collections || (yield prompt('Collections: '))) : [];
			options.output = options.action == 'export' ? program.output || (yield prompt('Output dir: ')) : '';
			options.from = options.action == 'import' ? program.from || (yield prompt('From: ')) : '';

			init();
		});
	})
	.parse(process.argv)

// Takes default command options if action is specified using a flag instead of explicitly action passed
if (program.import || program.export) {
	options = program;
	options.action = program.import 
				? 'import'
				: program.export
				? 'export'
				: ''
	init();
}


function init() {
	let allRight = validate();

	if (allRight) {

		if (options.action == 'export') 
			doExport(showSummary);
		else if (options.action == 'import')
			doImport(showSummary);
	}
	else
		process.exit(1);
}


/*
	Get collection names from db
*/
function getCollectionNames(callback) {

	let connection = (options.host || "localhost") + (options.port ? ":" + options.port : "") + "/" + options.database + (options.user ? " -u " + options.user : "") + (options.password ? " -p " + options.password : "");
	let command = 'mongo ' + connection + ' --eval "db.getCollectionNames().join()"';

	executeCommand(command, (result) => {
		let parts = result.stdout.split(options.database);
		let cols = parts[parts.length - 1].replace(/\r\n/g, '').split(',');

		if (result.error) {
			console.log("Cannot get collection names from database. ERROR: %s", result.error);
			process.exit(1);
		}
		else if (callback)
			callback(cols);
	});
}


/*
	Return a list of strings containing names of .json files from a directory
*/
function getCollectionsFromDirectory(dir, callback) {
	let dirCollections = [];

	fs.readdirSync(dir).forEach(file => {
		if (file.indexOf(".json")) {
			let collection = file.split(".")[0];
			dirCollections.push(collection);
		}
	});

	if (callback)
		callback(dirCollections);
}


/*
	Check required info
*/
function validate() {
	let validationErrors = [];

	if (!options.action)
		validationErrors.push("No action specified");

	if (!options.database)
		validationErrors.push("No database specified");

	if (options.action == 'export' && !options.output)
		validationErrors.push("No output directory specified");
	else if (options.action == 'import') {

		if (!options.from)
			validationErrors.push("No directory specified");
		else if (!fs.existsSync(options.from))
			validationErrors.push("Directory specified does not exists");
	}

	if (validationErrors.length) {
		console.log("error: ");
		
		for (var i = 0; i < validationErrors.length; i++) {
			console.log("	- %s", validationErrors[i]);
		}

		return false;
	}
	else
		return true;
}


/*
	Get import/export command for specified collection
*/
function getCommand(col, cb) {
	let command = "";

	if (options.action.toLowerCase() == 'import')
		command += "mongoimport";
	else if (options.action.toLowerCase() == 'export')
		command += "mongoexport";

	let host = "localhost";

	if (options.host) 
		host = options.host;

	if (options.port)
		host += (":" + options.port);

	command += (" --host " + host);

	if (options.user) {
		command += (" -u " + options.user)

		if (options.password)
			command += (" -p " + options.password)
	}

	command += (" -d " + options.database);
	command += (" -c " + col);

	if (options.output) {
		let output = "";

		if (options.output.indexOf(".json") == -1) 
			output += options.output + "/" + col + ".json";
		else {
			let parts =  options.output.split('.');
			output +=  (parts[parts.length - 2] + "_" + col + ".json");
		}

		command += (" -o " + output);
	}
	else if (options.from) {
		let from = "";

		if (options.from.indexOf(".json") != -1)
			from = options.from;
		else 
			from = options.from + "/" + col + ".json";

		command += (" " + from);
	}

	return command;
}


/*
	Executes a command as child process
	@param callback - action to do after process finish succesfully
	@param keepTrace - add/not add command to 'processed', 'errors' collections, also shows a message if an error occurs during executing the command 
*/
function executeCommand(command, callback, keepTrace) {
	exec(command, (err, stdout, stderr) => {
		let result = { stdout: stdout, stderr: stderr, error: null };

		if (err) {
			result.error = err;

			if (keepTrace) {
				console.info("Error during executing command: " + command + ". Omited");
				failed.push(command);
			}

			if (callback) 
				callback(result);

			return;
		}
		else {
			if (keepTrace) {
				console.log(command + " processed");
				processed.push(command);
			}

			if (callback)
				callback(result);
		}
	});
}


/*
	Executes requested action for each collection
*/
function doExport(callback) {
	function next(index) {
        let collection = options.collections[index];

        if (collection) {
        	let command = getCommand(collection);

        	if (command)
            	executeCommand(command, () => { next(index + 1) }, true);
            else 
            	next(index + 1);
        }
        else if (callback)
            callback();

        return;
    }

	if (!options.collections || !options.collections.length) {
		getCollectionNames((cols) => {
			options.collections = cols;

		    next(0);
		});
	}
	else
		next(0);
}


/*
	Import each collection in 'from' directory
*/
function doImport(callback) {
	let collections = [];

	if (options.from.indexOf(".json") == -1) {
		getCollectionsFromDirectory(options.from, (cols) => {
			collections = cols;
			next(0);
		});
	}
	else {
		collections.push(options.from.split('.')[0]);
		next(0);
	}

	
    function next(index) {
        let collection = collections[index];

        if (collection) {
        	let command = getCommand(collection);

        	if (command)
            	executeCommand(command, () => { next(index + 1) }, true);
            else 
            	next(index + 1);
        }
        else if (callback)
            callback();

        return;
    }
}


/*
	Show info about process
*/
function showSummary() {
	console.info("%s collections processed", processed.length);
	console.info("%s errors", failed.length);

	if (processed.length) {
		if (options.action == 'export')
			console.info("Database '%s' successfully exported to '%s'", options.database, options.output);
		else if (options.action == 'export')
			console.info("Imported collections from '%s' successfully to database '%s'", options.from, options.database);
	}

	process.exit();
}