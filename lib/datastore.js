var mongodb = require('mongodb')
  ,  mongoclient = mongodb.MongoClient

;

function DataStore (config){

  if (!config.name) throw new Error('invalid configuration: data store must have a name');

  if (!config.database) config.database = 'happn';

  if (!config.collection) config.collection = 'happn';

  Object.defineProperty(this, 'config', {value:config});
}

DataStore.prototype.ObjectID =  mongodb.ObjectID;

DataStore.prototype.initialize = function(callback){

  var _this = this;

  mongoclient.connect (_this.config.url + '/' + _this.config.database, _this.config.opts, function (err, database) {

    if (err) return callback(err);

    var collection = database.collection(_this.config.collection);

    collection.createIndex('path_index', {path: 1}, {unique: true, w: 1}, function (e) {

      if (e) return callback(e);

      Object.defineProperty(_this, 'data', {value:collection});

      callback();

    });
  });
};

DataStore.prototype.findOne = function(criteria, fields, callback){

  return this.data.findOne(criteria, fields, callback);
};

DataStore.prototype.find = function(criteria, searchOptions){

  return this.data.find(criteria, searchOptions);
};

DataStore.prototype.insert = function(data, callback){

  return this.data.insert(data, {}, callback);
};

DataStore.prototype.update = function(criteria, data, callback){

  return this.data.update(criteria, data, {upsert: true}, callback);
};

DataStore.prototype.findAndModify = function(criteria, data, callback){

  return this.data.findAndModify(criteria, null, data, {upsert: true, "new": true}, callback);
};

DataStore.prototype.remove = function(criteria, callback){

  return this.data.remove(criteria, {multi: true}, callback);
};

module.exports.create = function(config, callback){

  try{

    var store = new DataStore(config);

    store.initialize(function(e){

      if (e) return callback(e);

      callback(null, store);
    });

  }catch(e){
    callback(e);
  }
};