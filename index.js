var db = require('./lib/datastore')
  , async = require('async')
;

function MongoProvider (config){

  if (!config) config = {};

  var ConfigManager = require('./lib/config');

  var configManager = new ConfigManager();

  Object.defineProperty(this, 'config', {value:configManager.parse(config)});
}

MongoProvider.prototype.UPSERT_TYPE = {
  upsert:0,
  update:1,
  insert:2
};

MongoProvider.prototype.initialize = function(callback){

  var _this = this;

  require('./lib/datastore').create(_this.config, function(err, store){

    if (err) return callback(err);

    _this.db = store;

    _this.__createIndexes(_this.config, callback);
  });
};

MongoProvider.prototype.__createIndexes = function(config, callback){

  var _this = this;

  var doCallback = function(e){

    if (e) return callback(new Error('failed to create indexes: ' + e.toString(), e));
    callback();
  };

  try{

    if (config.index === false){

      console.warn('no path index configured for datastore with collection: ' + config.collection + ' this could result in duplicates and bad performance, please make sure all data items have a unique "path" property');

      doCallback();

    } else {

      if (config.index == null){
        config.index = {
          "happn_path_index":{
            fields:{path: 1},
            options:{unique: true, w: 1}
          }
        };
      }

      _this.find('/_SYSTEM/INDEXES/*', {}, function(e, indexes){

        if (e) return doCallback(e);

        //indexes are configurable, but we always use a default unique one on path, unless explicitly specified
        async.eachSeries(Object.keys(config.index), function(indexKey, indexCB){

          var found = false;

          indexes.every(function(indexConfig){
            if (indexConfig.path == '/_SYSTEM/INDEXES/' + indexKey) found = true;
            return !found;
          });

          if (found) return indexCB();

          var indexConfig = config.index[indexKey];

          _this.db.data.createIndex(indexConfig.fields, indexConfig.options, function(e, result){

            if (e) {
              //check if there is an index created error
              return indexCB(e);
            }

            _this.upsert('/_SYSTEM/INDEXES/' + indexKey, { data: indexConfig, creation_result:result }, {}, false, indexCB);
          });
        }, doCallback);
      });
    }

  }catch(e){
    doCallback(e);
  }
};

MongoProvider.prototype.escapeRegex = function (str) {

  if (typeof str !== 'string') throw new TypeError('Expected a string');

  return str.replace(/[|\\{}()[\]^$+*?.]/g, "\\$&");
};

MongoProvider.prototype.preparePath = function (path) {

  //strips out duplicate sequential wildcards, ie simon***bishop -> simon*bishop

  if (!path) return '*';

  var prepared = '';

  var lastChar = null;

  for (var i = 0; i < path.length; i++) {

    if (path[i] == '*' && lastChar == '*') continue;

    prepared += path[i];

    lastChar = path[i];
  }

  return prepared;
};

MongoProvider.prototype.getPathCriteria = function (path) {

  var pathCriteria = {$and: []};

  var returnType = path.indexOf('*'); //0,1 == array -1 == single

  if (returnType > -1) {

    //strip out **,***,****
    var searchPath = this.preparePath(path);

    searchPath = '^' + this.escapeRegex(searchPath).replace(/\\\*/g, ".*") + '$';

    pathCriteria.$and.push({'path': {$regex: new RegExp(searchPath)}});

  } else pathCriteria.$and.push({'path': path}); //precise match

  return pathCriteria;
};

// MongoProvider.prototype.getPathCriteria = function(path){
//
//   var pathCriteria = {$and: []};
//
//   var returnType = path.indexOf('*'); //0,1 == array -1 == single
//
//   if (returnType == 0) pathCriteria.$and.push({'path': {$regex: new RegExp(path.replace(/[*]/g, '.*'))}});//keys with any prefix ie. */joe/bloggs
//
//   else if (returnType > 0) pathCriteria.$and.push({'path': {$regex: new RegExp('^' + path.replace(/[*]/g, '.*'))}});//keys that start with something but any suffix /joe/*/bloggs/*
//
//   else pathCriteria.$and.push({'path': path}); //precise match
//
//   return pathCriteria;
// };

MongoProvider.prototype.findOne = function(criteria, fields, callback){

  return this.db.findOne(criteria, fields, callback);
};

MongoProvider.prototype.find = function(path, parameters, callback){

  var _this = this;

  var pathCriteria = _this.getPathCriteria(path);

  if (parameters.criteria) pathCriteria.$and.push(parameters.criteria);

  var searchOptions = {};

  var sortOptions = parameters.options?parameters.options.sort:null;

  if (parameters.options){

    if (parameters.options.fields) searchOptions.fields = parameters.options.fields;

    if (parameters.options.limit) searchOptions.limit = parameters.options.limit;
  }

  _this.db.find(pathCriteria, searchOptions, sortOptions, function(e, items){

    if (e) return callback(e);

    callback(null, items);
  });
};

MongoProvider.prototype.update = function(criteria, data, options, callback){

  return this.db.update(criteria, data, options, callback);
};

MongoProvider.prototype.__getMeta = function(response){

  var meta = {
    created:response.created,
    modified:response.modified,
    modifiedBy:response.modifiedBy,
    path:response.path,
    _id:response._id
  };

  return meta;
};

MongoProvider.prototype.upsert = function(path, setData, options, dataWasMerged, callback){

  var _this = this;

  var modifiedOn = Date.now();

  var setParameters = {
    $set: {'data': setData.data, 'path': path, modified:modifiedOn},
    $setOnInsert: {"created": modifiedOn}
  };

  if (options.modifiedBy) setParameters.$set.modifiedBy = options.modifiedBy;

  if (setData._tag) setParameters.$set._tag = setData._tag;

  if (!options) options = {};

  options.upsert = true;

  if (options.upsertType === _this.UPSERT_TYPE.insert){

    setParameters.$set.created = modifiedOn;

    return _this.db.insert(setParameters.$set, options, function (err, response) {

      if (err) return callback(err);

      callback(null, response, setParameters.$set, false, _this.__getMeta(response));

    }.bind(_this));
  }

  if (options.upsertType === _this.UPSERT_TYPE.update){

    return _this.db.update({'path': path}, setParameters, options, function (err, response) {

      if (err) return callback(err);

      callback(null, response, response, false, _this.__getMeta(response));

    }.bind(_this));
  }

  _this.db.findAndModify({'path': path}, setParameters, function (err, response) {

    if (err) return callback(err);

    callback(null, response, response, true, _this.__getMeta(response));

  }.bind(_this));
};

MongoProvider.prototype.remove = function(path, callback){

  return this.db.remove(this.getPathCriteria(path), function(e, removed){

    if (e) return callback(e);

    callback(null, {
      'data': {
        removed: removed.result.n
      },
      '_meta': {
        timestamp: Date.now(),
        path: path
      }
    });
  });
};

function BatchDataItem(options, db){

  this.options = options;
  this.queued = [];
  this.callbacks = [];
  this.db = db;
}

BatchDataItem.prototype.empty = function(){

  clearTimeout(this.timeout);

  var opIndex = 0;

  var _this = this;

  var emptyQueued = [];

  var callbackQueued = [];

  //copy our insertion data to local scope

  emptyQueued.push.apply(emptyQueued, this.queued);

  callbackQueued.push.apply(callbackQueued, this.callbacks);

  //reset our queues
  this.queued = [];

  this.callbacks = [];

  //insert everything in the queue then loop through the results
  _this.db.insert(emptyQueued, this.options, function(e, response){

    // do callbacks for all inserted items
    callbackQueued.forEach(function(cb){

      if (e) return cb.call(cb, e);

      cb.call(cb, null, {ops:[response.ops[opIndex]]});

      opIndex++;

    });

  }.bind(this));
};

BatchDataItem.prototype.insert = function(data, callback){

  this.queued.push(data);

  this.callbacks.push(callback);

  //epty the queue when we have reached our batch size
  if (this.queued.length >= this.options.batchSize) return this.empty();

  //as soon as something lands up in the queue we start up a timer to ensure it is emptied even when there is a drop in activity
  if (this.queued.length == 1) this.initialize();//we start the timer now
};

BatchDataItem.prototype.initialize = function(){

  //empty our batch based on the timeout
  this.timeout = setTimeout(this.empty.bind(this), this.options.batchTimeout);
};

var batchData = {};

MongoProvider.prototype.batchInsert = function(data, options, callback){

  var _this = this;

  options.batchTimeout = options.batchTimeout || 500;

  //keyed by our batch sizes
  if (!batchData[options.batchSize]) batchData[options.batchSize] = new BatchDataItem(options, _this.db);

  batchData[options.batchSize].insert(data, callback);

};

MongoProvider.prototype.insert = function(data, options, callback){

  if (options.batchSize > 0) return this.batchInsert(data, options, callback);

  this.db.insert(data, options, callback);
};

MongoProvider.prototype.startCompacting = function (interval, callback, compactionHandler) {
  return callback();
};

MongoProvider.prototype.stopCompacting = function (callback) {
  return callback();
};

MongoProvider.prototype.compact = function (callback) {
  return callback();
};

MongoProvider.prototype.stop = function(callback){
  this.db.disconnect(callback);
};

module.exports = MongoProvider;
