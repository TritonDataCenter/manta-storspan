/*
 * manta-storspan.js: server program to help identify a unique set of Manta
 * storage ids
 */

var mod_assertplus = require('assert-plus');
var mod_bunyan = require('bunyan');
var mod_cmdutil = require('cmdutil');
var mod_getopt = require('posix-getopt');
var mod_http = require('http');
var mod_jsprim = require('jsprim');
var mod_manta = require('manta');
var mod_net = require('net');
var mod_os = require('os');
var mod_stream = require('stream');
var mod_util = require('util');
var mod_vasync = require('vasync');
var VError = require('verror');

/*
 * Configuration
 */

/* IP address for this process's host that's reachable from within the job  */
var address;

/* TCP port to use for our HTTP server */
var port = 1725;

/* Number of distinct servers seen */
var nunique = 0;

/* Number of objects we've started creating. */
var nobjects = 0;

/* Number of objects queued. */
var nenqueued = 0;

/* Maximum number of outstanding create-runtask operations outstanding. */
var concurrency = 10;

/* Number of storage nodes we expect to find. */
var nmaxnodes;

/* Maximum number of requests that we'll consider making. */
var nmaxrequests;

/* Root of our working Manta directory path */
var root = [ '', process.env['MANTA_USER'], 'stor',
    'manta-storspan' ].join('/');


/*
 * Internal state
 */

var log, server, manta, queue, mjob;

/*
 * Set of objects we've created and submitted to the job, but have not yet heard
 * about.
 */
var objectsWaiting = {};

/*
 * Set of server uuids that we've seen and the names of the object located on
 * them.
 */
var serversSeen = {};

var usageMessage = [
    '',
    'Creates test objects in Manta, locates them using a Manta job, and',
    'repeats the process until at least one object has been created on each',
    'storage server.  Upon completion, there will be one object per storage',
    'server, named according to the server\'s unique identifier.  Both test',
    'objects and the final output objects are stored in',
    '"/$MANTA_USER/stor/manta-storspan".',
    '',
    'REQUIRED ARGUMENTS',
    '',
    '    You must specify the environment variables MANTA_URL, MANTA_USER,',
    '    and MANTA_KEY_ID as you would to use any other Manta command-line',
    '    client.',
    '',
    '    You must specify IP, an IP address for the system from which you are',
    '    running this tool.  This IP address must be reachable from inside a',
    '    Manta job.  The easiest way to manage this for a Manta deployment',
    '    whose jobs have connectivity to the internet is to run this program',
    '    from a system with a public IP address and use that, though there is',
    '    in principle some danger running any unhardened server program',
    '    (including this one) from a public address.',
    '',
    '    You must specify NSERVERS, the total number of distinct storage',
    '    servers expected in this Manta deployment.  The program will stop',
    '    when that number of servers has been found or too many requests have',
    '    been made given the expected number of servers.',
    '',
    'OPTIONS',
    '',
    '    -c / --concurrency N    process N objects at a time',
    '    -o / --output PATH      put test and final output objects into PATH',
    '                            instead of /$MANTA_USER/stor/manta-storspan.',
    '    -p / --port PORT        internal server should listen on TCP port',
    '                            PORT'
].join('\n');

function main()
{
	var parser, option, u, args;

	mod_cmdutil.configure({
	    'synopses': [ '[OPTIONS] IP NSERVERS' ],
	    'usageMessage': usageMessage
	});

	parser = new mod_getopt.BasicParser([
	    'c:(concurrency)',
	    'h(help)',
	    'o:(output)',
	    'p:(port)'
	].join(''), process.argv);

	while ((option = parser.getopt()) !== undefined) {
		switch (option.option) {
		case 'c':
			u = parseInt(option.optarg, 10);
			if (isNaN(u) || u <= 0) {
				mod_cmdutil.usage(
				    'invalid value for -c/--concurrency: "%s"',
				    option.optarg);
			}
			concurrency = u;
			break;

		case 'h':
			mod_cmdutil.usage();
			break;

		case 'o':
			root = option.optarg;
			break;

		case 'p':
			u = parseInt(option.optarg, 10);
			if (isNaN(u) || u <= 0 || u >= 65536) {
				mod_cmdutil.usage(
				    'invalid value for -p/--port: "%s"',
				    option.optarg);
			}
			port = u;
			break;

		default:
			/* error message already emitted by getopt */
			mod_assertplus.equal('?', option.option);
			mod_cmdutil.usage();
			break;
		}
	}

	args = process.argv.slice(parser.optind());
	if (args.length < 2) {
		mod_cmdutil.usage('missing required arguments');
	}

	if (args.length > 2) {
		mod_cmdutil.usage('unexpected arguments');
	}

	if (!mod_net.isIP(args[0])) {
		mod_cmdutil.usage('unsupported address: "%s"', args[0]);
	}
	address = args[0];

	u = parseInt(args[1], 10);
	if (isNaN(u) || u <= 0) {
		mod_cmdutil.usage('unsupported value for NSERVERS: "%s"',
		    args[1]);
	}

	nmaxnodes = u;
	nmaxrequests = 10 * nmaxnodes;

	log = new mod_bunyan({
	    'name': 'manta-storspan',
	    'level': process.env['LOG_LEVEL'] || 'info',
	    'stream': process.stderr
	});

	server = mod_http.createServer(onCallbackFromJob);
	server.listen(port, function () {
		log.debug(server.address(), 'server listening');
	});

	manta = mod_manta.createBinClient({
	    'log': log.child({ 'component': 'MantaClient' })
	});
	process.removeAllListeners('uncaughtException');

	queue = mod_vasync.queuev({
	    'concurrency': concurrency,
	    'worker': makeFile
	});

	queue.on('end', function () {
		log.debug('shutting down');
		server.close();

		console.log('found %d node%s (expected %d)',
		    nunique, nunique == 1 ? '' : 's', nmaxnodes);
		if (nunique < nmaxnodes && nenqueued >= nmaxrequests) {
			console.log('(gave up after %d requests)', nenqueued);
		}

		log.debug('cancelling job %s', mjob);
		console.log('cancelling job %s', mjob);
		manta.cancelJob(mjob, function (err) {
			if (err) {
				err = new VError(err, 'cancel job "%s"', mjob);
				mod_cmdutil.fail(err);
			}

			manta.close();
		});
	});

	mod_vasync.waterfall([
	    function mssInit(callback) {
		log.debug({ 'root': root }, 'checking root');
		manta.info(root, function onHeadRoot(err) {
			if (!err) {
				callback(new VError(
				    '%s already exists in Manta', root));
			} else if (err.name != 'NotFoundError') {
				callback(new VError(err, 'HEAD "%s"', root));
			} else {
				callback();
			}
		});
	    },

	    function mssMkdirp(callback) {
		log.debug({ 'root': root }, 'mkdirp');
		manta.mkdirp(root, function (err) {
			if (err) {
				err = new VError(err, 'mkdirp "%s"', root);
			}

			callback(err);
		});
	    },

	    function mssCreateJob(callback) {
		var jobcfg = {
		    'name': mod_util.format('manta-storspan: %s pid %d',
		        mod_os.hostname(), process.pid),
		    'phases': [ {
			'type': 'map',
		        'exec': 'echo "$MANTA_INPUT_OBJECT" | ' +
			    'curl -T- http://' + address + ':' + port +
			    '/manta-storspan/"$(mdata-get sdc:server_uuid)"'
		    } ]
		};

		log.debug(jobcfg, 'creating job');
		manta.createJob(jobcfg, function onJobCreated(err, jobinfo) {
			if (err) {
				callback(new VError(err, 'create job'));
				return;
			}

			mjob = jobinfo;
			console.log('created job %s', jobinfo);
			callback();
		});
	    }
	], function onWaterfallDone(err) {
		var i;

		if (err) {
			mod_cmdutil.fail(err);
		}

		for (i = 0; i < nmaxnodes; i++) {
			queue.push({ 'i': nenqueued++ });
		}
	});
}

function makeFile(state, queuecallback)
{
	mod_assertplus.object(state);
	mod_assertplus.func(queuecallback);
	mod_assertplus.number(state.i);

	state.manta = manta;
	state.objectPath = [ root, 'obj' + state.i ].join('/');
	state.log = log.child({
	    'objectPath': state.objectPath
	});

	state.pipeline = mod_vasync.pipeline({
	    'arg': state,
	    'funcs': [
		function mkOneObject(st, callback) {
			var stream = new mod_stream.PassThrough();
			var options = { 'size': 1, 'copies': 1 };
			stream.end('\n');
			st.manta.put(st.objectPath, stream, options,
			    function onPutDone(err) {
				if (err) {
					err = new VError(err,
					    'put "%s"', st.objectPath);
				}

				callback(err);
			    });
		},

		function addToJob(st, callback) {
			mod_assertplus.ok(!mod_jsprim.hasKey(
			    objectsWaiting, st.objectPath));
			objectsWaiting[st.objectPath] = {
			    'callback': callback,
			    'duplicate': null,
			    'serverUuid': null
			};

			st.manta.addJobKey(mjob,
			    st.objectPath, { 'end': false },
			    function onInputAdded(err) {
				if (err) {
					err = new VError(err,
					    'adding input "%s"',
					    st.objectPath);
					delete (objectsWaiting[st.objectPath]);
					callback(err);
				}

				/*
				 * The callback will not be invoked in the
				 * success case until the job calls back to us.
				 */
			    });
		},

		function gotJobResult(st, callback) {
			var record;
			mod_assertplus.ok(mod_jsprim.hasKey(
			    objectsWaiting, st.objectPath));

			record = st.record = objectsWaiting[st.objectPath];
			delete (objectsWaiting[st.objectPath]);
			mod_assertplus.bool(record.duplicate);
			mod_assertplus.string(record.serverUuid);

			if (record.duplicate) {
				log.debug({
				    'serverUuid': record.serverUuid,
				    'objectPath': st.objectPath,
				    'nunique': nunique
				}, 'duplicate');

				callback();
			} else {
				serversSeen[record.serverUuid] = st.objectPath;
				nunique++;
				log.debug({
				    'serverUuid': record.serverUuid,
				    'objectPath': st.objectPath,
				    'nunique': nunique
				}, 'found new server');

				st.serverObjectPath = [
				    root, record.serverUuid ].join('/');
				st.manta.ln(st.objectPath, st.serverObjectPath,
				    function onLinkDone(err) {
					if (err) {
						err = new VError(err,
						    'snaplink "%s" to "%s"',
						    st.objectPath,
						    st.serverObjectPath);
					}

					callback(err);
				    });
			}
		},

		function removeOriginal(st, callback) {
			st.manta.unlink(st.objectPath,
			    function onUnlinkDone(err) {
				if (err) {
					err = new VError(err,
					    'unlink "%s"', st.objectPath);
				}

				callback(err);
			    });
		}
	    ]
	}, function (err) {
		if (err) {
			mod_cmdutil.fail(err);
		}

		/*
		 * Note that it's still possible to succeed because of the
		 * enqueued requests, so we don't check this condition for final
		 * reporting until the queue actually finishes.
		 */
		if (nunique >= nmaxnodes) {
			log.debug('shutting down because done');
			queue.close();
		} else if (nenqueued >= nmaxrequests) {
			log.debug('shutting down because giving up');
			queue.close();
		} else {
			log.debug('enqueueing another object');
			queue.push({ 'i': nenqueued++ });
		}

		queuecallback();
	});
}

function onCallbackFromJob(request, response)
{
	var pathparts, body, rqlog;

	rqlog = log.child({
	    'component': 'server',
	    'method': request.method,
	    'url': request.url
	});

	rqlog.debug('incoming request');
	if (request.method != 'PUT') {
		rqlog.warn('bad request: bad method');
		response.writeHead(405);
		response.end('bad request\n');
		return;
	}

	pathparts = request.url.split('/');
	mod_assertplus(pathparts.length > 1);
	mod_assertplus(pathparts[0] === '');
	if (pathparts.length != 3) {
		rqlog.warn('bad request: bad path');
		response.writeHead(400);
		response.end('bad request\n');
		return;
	}

	if (pathparts[1] != 'manta-storspan') {
		rqlog.warn('bad request: bad top-level path');
		response.writeHead(400);
		response.end('bad request: bad top\n');
		return;
	}

	body = '';
	request.on('data', function (c) {
		/* XXX make sure it's not too big */
		body += c.toString('utf8');
	});
	request.on('end', function () {
		rqlog.debug('request okay');
		response.writeHead(204);
		response.end();
		onObject({
		    'serverUuid': pathparts[2],
		    'objectPath': body.trim()
		});
	});
}

function onObject(args)
{
	var serveruuid, objectpath, record;

	serveruuid = args.serverUuid;
	objectpath = args.objectPath;

	if (!mod_jsprim.hasKey(objectsWaiting, objectpath)) {
		log.warn({
		    'objectPath': objectpath
		}, 'ignoring callback from job for unknown object');
		return;
	}

	record = objectsWaiting[objectpath];
	record.serverUuid = serveruuid;
	record.duplicate = mod_jsprim.hasKey(serversSeen, serveruuid);
	record.callback();
}

main();
