var url = require('url')

  ;

function Config(){

}

Config.prototype.parse = function(config){

  var parsed = JSON.parse(JSON.stringify(config));

  if (!parsed.database) parsed.database = 'happn';

  if (!parsed.collection) parsed.collection = 'happn';

  if (!parsed.url) parsed.url = 'mongodb://127.0.0.1:27017';

  else {

    var defaultOpts = this.getDBConnectionOpts(parsed.url, parsed.database, parsed.collection);

    parsed.url = defaultOpts.url;
    parsed.collection = defaultOpts.collection;
  }

  if (!parsed.datastores || parsed.datastores.length == 0) {

    parsed.datastores = [{

      name:'default',
      url:parsed.url,
      database:parsed.database,
      collection:parsed.collection
    },{

      name:'system',
      url:parsed.url,
      database:parsed.database,
      collection:'happn-system',
      patterns:['/_SYSTEM/*']

    }];
  }

  return parsed;

};

Config.prototype.getDBConnectionOpts = function(connUrl, databaseName, collectionName){

  var urlParts = url.parse(connUrl);

  var config = {};

  if (urlParts.pathname){

    var pathParts = urlParts.pathname.split('/');

    if (pathParts.length == 2 && pathParts[1] != ''){
      //we have a collection in the url

      config.url = urlParts.protocol + '//' + urlParts.host;
      config.databaseName = pathParts[1];

    }
  } else config.url = connUrl;

  //defaults

  if (databaseName && !config.database) config.database = databaseName;

  if (collectionName && !config.collection) config.collection = collectionName;

  return config;
};

module.exports = Config;