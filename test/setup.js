var
	CassandraProvider = require("../index"),
	config = require("../config.json")
;

exports.HttpCache = require("http-cache");
exports.provider = new CassandraProvider(config);
