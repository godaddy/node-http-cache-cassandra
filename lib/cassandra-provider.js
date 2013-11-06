var
	assert = require("assert"),
	extend = require("extend"),
	util = require("util"),
	helenus = require("helenus")
;

module.exports = exports = CassandraProvider;

/* Tables
	CREATE TABLE httpCache
	(
		key text,
		cache text,
		expires bigint,
		PRIMARY KEY (key)
	);
*/

function CassandraProvider(options)
{
	if (!(this instanceof CassandraProvider)) {
		return new CassandraProvider();
	}

	var defaults = {
		hosts: ['localhost:9160'],
		timeout: 4000,
		cqlVersion: '3.0.0'
	};
	this.options = extend(true, defaults, options || {});
	this.pools = {};
	
	assert.ok(this.options, "options REQUIRED");
	assert.ok(this.options.hosts, "options.hosts REQUIRED");
	assert.ok(this.options.keyspace, "options.keyspace REQUIRED");
	assert.ok(this.options.user, "options.user REQUIRED");
	assert.ok(this.options.password, "options.password REQUIRED");
	
	// auto-create TABLE if does not already exist?
	/*var $this = this;
	execCQL.call(this, "CREATE TABLE httpCache (key text, cache text, expires bigint, PRIMARY KEY (key));", [], function(err) {
	});*/
	
	// inform callers that we manage the TTL
	this.isTTLManaged = true;
}

var p = CassandraProvider.prototype;

p.get = function(key, cb) {
	var args = arguments;
	execCQL.call(this, "SELECT cache FROM httpCache WHERE key = ?",
		[key],
		function(err, results) {
			var ret = null;
			if (results && results.length > 0) {
				ret = results[0].cache;//JSON.parse(results[0].replace(/'/g, "\'"));
				if (ret && util.isArray(ret.body) === true) {
					ret.body = new Buffer(ret.body);
				}
			}

			cb && cb(err, ret);
		}
	);
};

p.set = function(key, cache, ttl, cb) {
	var args = arguments;
	var val = typeof cache === "object" ? JSON.stringify(cache) : cache;
	execCQL.call(this,
		"INSERT INTO httpCache (key, cache, expires) VALUES (?, ?, ?) USING TTL ?",
		[key, val, (Date.now() + (ttl * 1000)), ttl],
		{ consistency: helenus.ConsistencyLevel.ALL },
		function(err, results) {
			cb && cb(err, results);
		}
	);
};

p.remove = function(key, cb) {
	var args = arguments;
	execCQL.call(this, "DELETE FROM httpCache WHERE key = ?",
		[key],
		{ consistency: helenus.ConsistencyLevel.ALL },
		function(err) {
			cb && cb(err);
		}
	);
};

/*
	DELETE FROM 'httpCache' WHERE expires < 12345
*/
p.clear = function(cb) {
	var args = arguments;
	execCQL.call(this, "TRUNCATE httpCache",
		[],
		{ consistency: helenus.ConsistencyLevel.ALL },
		function(err) {
			cb && cb(err);
		}
	);
};

function execCQL(cql, data, options, cb) {
	if (typeof options === "function") {
		cb = options;
		options = {};
	}
	options = options || {};
	var consistency = options.consistency || helenus.ConsistencyLevel.ONE;
	var pool = this.pools[consistency];
	var $this = this;
	if (!pool) {
		var poolConfig = extend(true, this.options, { consistencyLevel: consistency || helenus.ConsistencyLevel.ONE });
		pool = this.pools[consistency] = new helenus.ConnectionPool(poolConfig);
		pool.waiters = [];
		pool.isReady = false;
		pool.on('error', this.onError || onError); // use custom onError if available, otherwise default to stdout
		pool.connect(function (err, keyspace) {
			pool.isReady = true;
			var i;
			for (i = 0; i < pool.waiters.length; i++) {
				pool.waiters[i](); // execute callback
			}
			pool.waiters = []; // reset
		});
		// TODO!!! account for reconnections as well
	}
	if (pool.isReady === false) {
		pool.waiters.push(function() {
			execCQL.call($this, cql, data, options, cb);
		});
		return;
	}
	var data_copy;
	if (consistency === helenus.ConsistencyLevel.ALL) { // if not ALL are available, fallback to QUORUM
		// client removes used data entries, so we need to create a copy of the data in case it's needed for retry...
		data_copy = new Array(data.length);
		for (i = 0; i < data.length; i++) {
			data_copy[i] = data[i];
		}
	}
	
	pool.cql(cql, data, function (err, results) {
		if (err) {
			if (consistency === helenus.ConsistencyLevel.ALL) { // if not ALL are available, fallback to QUORUM
				options.consistency = helenus.ConsistencyLevel.QUORUM;
				execCQL.call($this, cql, data_copy, options, function(err, results) {
					if (results && typeof results.length === "number") {
						results = getNormalizedResults(results);
					}

					cb && cb(err, results);
				});
				return;
			}
		}

		if (results && typeof results.length === "number") {
			results = getNormalizedResults(results);
		}

		cb && cb(err, results);
	});
}

function onError(err) {
	// log to stderr? probably should persist to a global logger once avail
	console.error("cassandra.helenus.onError: ", err, util.inspect(err.stack, {depth: null}));
}

function getNormalizedResults(original) {
	var i, k, v, row, result, results;

	if (util.isArray(original) === true) {
		results = new Array(original.length);
		for (i = 0; i < original.length; i++) {
			row = original[i];
			result = {};
			for (k in row._map) {
				v = row.get(k);
				if (v && v.value) {
					v = v.value;
					if (typeof v === "string" && v.length > 0 && (v[0] === "{" || v[0] === "[" || v === "null")) {
						// attempt to auto-deserialize
						try {
							v = JSON.parse(v);
						} catch (ex) {
							// ignore
						}
					}
					/*if (Buffer.isBuffer(v) === true) {
						// convert
						v = v.toString('utf8');
					}*/
					result[k] = v;
				}
			}
			results[i] = result;
		}
	} else {
		results = [];
	}

	return results;
}
