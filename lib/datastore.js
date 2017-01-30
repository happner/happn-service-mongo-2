var mongodb = require('mongodb')
  ,  mongoclient = mongodb.MongoClient
  ,  async = require('async')

;

function DataStore (config){

  if (!config.name) throw new Error('invalid configuration: data store must have a name');

  if (!config.database) config.database = 'happn';

  if (!config.collection) config.collection = 'happn';

  if (!config.policy) config.policy = {};

  if (config.cache) {

    if (config.cache === true) config.cache = {};

    if (!config.cache.cacheId) config.cache.cacheId = config.name;
  }

  Object.defineProperty(this, 'config', {value:config});
}

DataStore.prototype.ObjectID =  mongodb.ObjectID;

DataStore.prototype.setUpCache = function(callback){

  var _this = this;

  var Cache = require('redis-lru-cache');

  var cache = new  Cache(_this.config.cache);

  Object.defineProperty(this, 'cache', {value:cache});

  _this.__oldFind = _this.find;

  _this.find = function(criteria, searchOptions, sortOptions, callback){

    if (criteria.path.indexOf && criteria.path.indexOf('*') == -1){

      return _this.cache.get(criteria.path, function(e, item){

        if (e) return callback(e);

        if (item) return callback(null, [item]);

        _this.__oldFind(criteria, searchOptions, sortOptions, function(e, items){

          if (e) return callback(e);

          if (!items || items.length == 0) return callback(null, []);

          _this.cache.set(criteria.path, items[0], function(e){

            return callback(e, items);
          });
        });
      });
    }
    return this.__oldFind(criteria, searchOptions, sortOptions, callback);
  }.bind(this);

  this.__oldFindOne = this.findOne;

  _this.findOne = function(criteria, fields, callback){

    _this.cache.get(criteria.path, function(e, item){

      if (e) return callback(e);
      if (item) return callback(null, item);

      _this.__oldFindOne(criteria, fields, function(e, item){

        if (e) return callback(e);
        if (!item) return callback(null, null);

        _this.cache.set(criteria.path, item, function(e){

          return callback(e, item);
        });
      });
    });

  }.bind(this);

  this.__oldFindAndModify = this.findAndModify;

  this.findAndModify = function(criteria, data, callback){
    //this is actually a set or update
    _this.__oldFindAndModify(criteria, data, function(e, item){

      if (e) return callback(e);

      if (item) return _this.cache.set(criteria.path, item, function(e){

        callback(e, item);
      });

      callback(null, null);
    });
  };

  this.__oldRemove = this.remove;

  this.remove = function(criteria, callback){

    if (criteria.path.indexOf && criteria.path.indexOf('*') == -1){

      //its ok if the actual remove fails, as the cache will refresh
      _this.cache.remove(criteria.path, function(e){

        if (e) return callback(e);

        _this.__oldRemove(criteria, callback);
      })

    } else {

      _this.find(criteria, {fields:{path:1}}, null, function(e, items){

        if (e) return callback(e);

        //clear the items from the cache
        async.eachSeries(items, function(item, itemCB){

          _this.cache.remove(item.path, itemCB);

        }, function(e){

          if (e) return callback(e);

          _this.__oldRemove(criteria, callback);
        });
      });
    }
  };

  _this.__oldInsert = _this.insert;

  _this.insert = function(data, options, callback){

    if (typeof options == 'function'){
      callback = options;
      options = null;
    }

    if (!options) options = {};

    _this.__oldInsert(data, function(e, response){

      if (e) return callback(e);

      if (options.noCache) return callback(null, response);

      _this.cache.set(data.path, data, function(e){

        if (e) return callback(e);

        return callback(null, response);
      });
    });
  };

  this.__oldUpdate = this.update;

  _this.update = function(criteria, data, options, callback){

    _this.__oldUpdate(criteria, data, options, function(e, response){

      if (e) return callback(e);

      _this.__cache.set(criteria.path, data, function(e){

        if (e) return callback(e);

        return callback(null, response);

      });
    });
  };

  callback();
};

DataStore.prototype.initialize = function(callback){

  var _this = this;

  mongoclient.connect (_this.config.url + '/' + _this.config.database, _this.config.opts, function (err, database) {

    if (err) return callback(err);

    var collection = database.collection(_this.config.collection);

    var doCallback = function(){

      Object.defineProperty(_this, 'data', {value:collection});

      if (_this.config.cache) _this.setUpCache(callback);
      else callback();
    };

    if (_this.config.index === false){

      console.warn('no index configured for datastore with collection: ' + _this.config.collection + ' this could result in duplicates, please make sure all data items have a unique "path" property');

      doCallback();

    } else {

      if (_this.config.index == null){
        _this.config.index = {
          "happn_path_index":{
            fields:{path: 1},
            options:{unique: true, w: 1}
          }
        };
      }

      //indexes are configurable, but we always use a default unique one on path, unless explicitly specified
      async.eachSeries(Object.keys(_this.config.index), function(indexKey, indexCB){

        var indexConfig = _this.config.index[indexKey];

        collection.createIndex(indexKey, indexConfig.fields, indexConfig.options, indexCB);

      }, function(e){

        if (e) return callback(e);

        doCallback();

      });
    }
  });
};

DataStore.prototype.findOne = function(criteria, fields, callback){

  return this.data.findOne(criteria, fields, callback);
};

DataStore.prototype.find = function(criteria, searchOptions, sortOptions, callback){

  if (!sortOptions) return this.data
    .find(criteria, searchOptions).toArray(callback);

  this.data
    .find(criteria, searchOptions)
    .sort(sortOptions)
    .toArray(callback);
};

var batchData = {};

function BatchDataItem(options, data){

  this.options = options;
  this.queued = [];
  this.callbacks = [];
  this.data = data;

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
  _this.data.insert(emptyQueued, this.options, function(e, response){

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

DataStore.prototype.batchInsert = function(data, options, callback){

  var _this = this;

  options.batchTimeout = options.batchTimeout || 500;

  //keyed by our batch sizes
  if (!batchData[options.batchSize]) batchData[options.batchSize] = new BatchDataItem(options, _this.data);

  batchData[options.batchSize].insert(data, callback);

};

DataStore.prototype.insert = function(data, options, callback){

  if (this.config.policy.set){
    for(var option in this.config.policy.set){
      if (options[option] === undefined) options[option] = this.config.policy.set[option];
    }
  }

  if (options.batchSize > 0) return this.batchInsert(data, options, callback);

  this.data.insert(data, options, callback);
};

DataStore.prototype.update = function(criteria, data, options, callback){

  return this.data.update(criteria, data, {upsert: true}, callback);
};

DataStore.prototype.findAndModify = function(criteria, data, callback){

  return this.data.findAndModify(criteria, null, data, {upsert: true, "new": true}, function(e, item){
    if (e) return callback(e);
    if (item) return callback(null, item.value);
    callback(null, null);
  });
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