# manta-storspan: create a minimal set of Manta objects spanning all storage servers

Creates test objects in Manta, locates them using a Manta job, and repeats the
process until at least one object has been created on each storage server.  Upon
completion, there will be one object per storage server, named according to the
server's unique identifier.  Both test objects and the final output objects are
stored in `/$MANTA_USER/stor/manta-storspan`.

This program is primarily of interest to Manta operators for exercising the data
path or jobs path with specific backend storage servers.  The program is a
normal, external Manta client.  By design, it does not use any internal Manta
interfaces.

Note that this creates an object on each server, not each storage zone.  If a
server has multiple storage zones, not all of these zones will be covered.


## Installation

    $ git clone https://github.com/joyent/manta-storspan
    $ cd manta-storspan
    $ npm install


## Synopsis

    export MANTA_URL=...
    export MANTA_USER=...
    export MANTA_KEY_ID=...
    node mstorspan.js [OPTIONS] IP NSERVERS


## Required arguments

You must specify the environment variables `MANTA_URL`, `MANTA_USER`, and
`MANTA_KEY_ID` as you would to use any other Manta command-line client.

You must specify `IP`, an IP address for the system from which you are running
this tool.  This IP address must be reachable from inside a Manta job.  The
easiest way to manage this for a Manta deployment whose jobs have connectivity
to the internet is to run this program from a system with a public IP address
and use that, though there is in principle some danger running any unhardened
server program (including this one) from a public address.

You must specify `NSERVERS`, the total number of distinct storage servers
expected in this Manta deployment.  The program will stop when that number of
servers has been found or too many requests have been made given the expected
number of servers.


## Options

    -c / --concurrency N    process N objects at a time
    -o / --output PATH      put test and final output objects into PATH
                            instead of /$MANTA_USER/stor/manta-storspan.
    -p / --port PORT        internal server should listen on TCP port
                            PORT

## Examples

A development environment typically has only one storage server, so the output
would look like this:

    $ node mstorspan.js 172.26.10.14 1
    created job 025a1bc6-35c3-e218-d0dc-b602c502bff8
    found 1 node (expected 1)
    cancelling job 025a1bc6-35c3-e218-d0dc-b602c502bff8

    $ mls -l /poseidon/stor/manta-storspan
    -rwxr-xr-x 1 poseidon             1 Feb 09 16:18 44454c4c-5700-1047-8051-b3c04f585131

If you run it again immediately, it will bail out because the output directory
already exists:

    $ node mstorspan.js 172.26.10.14 1
    mstorspan.js: /poseidon/stor/manta-storspan already exists in Manta

You can deal with this by using the `--output` option to specify a different
directory or remove the default one.

If you specify a number of servers that's too low, the program will stop early
thinking it's found all of them.  If you specify a number that's too large, the 
program will take a long time to complete (since it can never complete
successfully) and eventually give up:

    $ node mstorspan.js 172.26.10.14 3
    created job 2d26a57d-3c2a-e305-cebb-b53980cb2613
    found 1 node (expected 3)
    (gave up after 30 requests)
    cancelling job 2d26a57d-3c2a-e305-cebb-b53980cb2613


## TODO

- periodically check the job for errors
