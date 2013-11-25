# http-cache

[![NPM version](https://badge.fury.io/js/http-cache-cassandra.png)](http://badge.fury.io/js/http-cache-cassandra)


## About

A Cassandra provider for the extensible HTTP caching library ```http-cache```.

See the core http-cache project for full documentation:

https://npmjs.org/package/http-cache



## Install

	npm install http-cache
	npm install http-cache-cassandra
	
https://npmjs.org/package/http-cache-cassandra


## Create Table

	CREATE TABLE httpCache
	(
        host text,
        path text,
		cache text,
		expires bigint,
		PRIMARY KEY (host, path)
	);



## Tests & Code Coverage

Install latest:

	git clone https://github.com/godaddy/node-http-cache-cassandra.git
	cd node-http-cache-cassanra
	npm install

Rename 'config.json.COPY' to 'config.json' and provide the proper credentials for testing.

	npm test

Now you can view coverage using any browser here:

	coverage/lcov-report/index.html



## License

[MIT](https://github.com/godaddy/node-http-cache-cassandra/blob/master/LICENSE.txt)



