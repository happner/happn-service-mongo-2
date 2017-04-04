var mongoUrl = require('parse-mongo-url');

function Config() {

}

Config.prototype.parse = function (config) {

  var parsed = JSON.parse(JSON.stringify(config));

  if (!parsed.database) parsed.database = 'happn';

  if (!parsed.collection) parsed.collection = 'happn';

  if (!parsed.url) parsed.url = 'mongodb://127.0.0.1:27017';

  else {

    var defaultOpts = this.getDBConnectionOpts(parsed.url, parsed.database, parsed.collection);

    parsed.url = defaultOpts.url;
  }

  return parsed;
};

Config.prototype.getDBConnectionOpts = function (connUrl, databaseName, collectionName) {

  var mongoUrlParts = mongoUrl(connUrl);

  var config = {};

  if (databaseName && databaseName != mongoUrlParts.dbName) {
    // we need to add the db name
    config.url = 'mongodb://';
    var servers = [];
    if (mongoUrlParts.auth) config.url += mongoUrlParts.auth.user + ':' + mongoUrlParts.auth.password + '@';
    for (var i in mongoUrlParts.servers) {
      servers[i] = mongoUrlParts.servers[i].host + ':' + mongoUrlParts.servers[i].port;
    }
    config.url += servers.join(',');
    config.url += '/';
    config.url += databaseName;
    if (connUrl.indexOf('?') !== -1) config.url += connUrl.substring(connUrl.indexOf('?'));
  }
  else config.url = connUrl;

  //defaults

  if (databaseName && !config.database) config.database = databaseName;
  if (collectionName && !config.collection) config.collection = collectionName;

  return config;
};

module.exports = Config;