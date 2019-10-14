var mongoUrl = require('parse-mongo-url');

function Config() {}

Config.prototype.parse = function(config) {
  var parsed = JSON.parse(JSON.stringify(config));
  if (!parsed.database) parsed.database = 'happn';
  if (!parsed.collection)
    parsed.collection = parsed.opts && parsed.opts.collection ? parsed.opts.collection : 'happn';
  if (!parsed.url) parsed.url = 'mongodb://127.0.0.1:27017';
  if (!parsed.opts) parsed.opts = {};
  parsed.url = this.getDBConnectionUrl(parsed.url, parsed.database);

  return parsed;
};

Config.prototype.getDBConnectionUrl = function(connUrl, databaseName) {
  var url = connUrl;
  var mongoUrlParts = mongoUrl(connUrl);
  if (databaseName != null && databaseName != mongoUrlParts.dbName) {
    // we need to add the db name
    url = 'mongodb://';
    var servers = [];
    if (mongoUrlParts.auth)
      url += mongoUrlParts.auth.user + ':' + mongoUrlParts.auth.password + '@';
    for (var i in mongoUrlParts.servers) {
      servers[i] = mongoUrlParts.servers[i].host + ':' + mongoUrlParts.servers[i].port;
    }

    url += servers.join(',');
    url += '/';
    url += databaseName;
    if (connUrl.indexOf('?') !== -1) url += connUrl.substring(connUrl.indexOf('?'));
  }
  return url;
};

module.exports = Config;
