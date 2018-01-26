var mongodb = require('mongodb')
  , mongoclient = mongodb.MongoClient
  , async = require('async')
  , sift = require('sift')
  ;

function MongoDataStore(config) {

  if (!config.database) config.database = 'happn';

  if (!config.collection) config.collection = 'happn';

  if (!config.policy) config.policy = {};

  if (config.cache) {

    if (config.cache === true) config.cache = {};

    if (!config.cache.cacheId) config.cache.cacheId = config.name;
  }

  if (!config.opts) config.opts = {};

  //enforce an immediately returned error when we cannot access the db
  config.opts.bufferMaxEntries = 0;
  //dont understand why passing this in has no effect on increasing the amount of retries
  config.opts.autoReconnect = false;

  delete config.opts.collection;

  Object.defineProperty(this, 'config', {value: config});
}

MongoDataStore.prototype.ObjectID = mongodb.ObjectID;

MongoDataStore.prototype.connect = function (callback) {

  var _this = this;

  _this.CONNECTED = false;

  if (_this.connection) _this.connection.removeAllListeners();

  async.whilst(function () {
    return _this.CONNECTED == false && _this.DISCONNECTED == false;
  }, function (next) {

    mongoclient.connect(_this.config.url, _this.config.opts, function (err, database) {

      if (err) return setTimeout(next, 1000);

      _this.CONNECTED = true;

      var collection = database.collection(_this.config.collection);

      _this.data = collection;

      _this.connection = database;

      database.on('close', _this.__handleDisconnected.bind(_this));

      if (_this.config.cache) _this.setUpCache(next);
      else next();
    });

  }, function (e) {
    if (callback) callback(e);
  });
};

MongoDataStore.prototype.initialize = function (callback) {

  this.CONNECTED = false;
  this.DISCONNECTED = false;

  this.connect(callback);
};

MongoDataStore.prototype.__handleDisconnected = function () {

  this.CONNECTED = false;

  this.connect();
};

MongoDataStore.prototype.__handleAllEvents = function () {
  console.log('__handleAllEvents', arguments);
};

MongoDataStore.prototype.findOne = function (criteria, fields, callback) {

  return this.data.findOne(criteria, fields, callback);
};

MongoDataStore.prototype.__extractNot = function (criteria) {

  if (!criteria) return null;

  var separated = {
    cleaned: {$and: []},
    nots: {$and: []}
  };

  criteria.$and.forEach(function (criterion) {
    if (criterion.$not) {
      separated.nots.$and.push(criterion);
      separated.hasNots = true;
    } else separated.cleaned.$and.push(criterion);
  });

  return separated;
};

MongoDataStore.prototype.find = function (criteria, searchOptions, sortOptions, callback) {

  var separatedCriteria = this.__extractNot(criteria);//this is because the $not keyword works in nedb and sift, but not in mongo

  var doCallback = function (e, items) {

    try {

      if (e) return callback(e);

      if (separatedCriteria.hasNots) callback(null, sift(separatedCriteria.nots, items));
      else callback(null, items);

    } catch (e) {
      callback(e);
    }
  };

  if (!sortOptions) this.data
    .find(separatedCriteria.cleaned, searchOptions).toArray(doCallback);

  else this.data
    .find(separatedCriteria.cleaned, searchOptions)
    .sort(sortOptions)
    .toArray(doCallback);
};

MongoDataStore.prototype.insert = function (data, options, callback) {

  return this.data.insert(data, options, callback);
};

MongoDataStore.prototype.update = function (criteria, data, options, callback) {

  return this.data.update(criteria, data, {upsert: true}, callback);
};

MongoDataStore.prototype.findAndModify = function (criteria, data, callback) {

  return this.data.findAndModify(criteria, null, data, {upsert: true, "new": true}, function (e, item) {

    if (e) return callback(e);
    if (item) return callback(null, item.value);
    callback(null, null);
  });
};

MongoDataStore.prototype.remove = function (criteria, callback) {

  return this.data.remove(criteria, {multi: true}, callback);
};

MongoDataStore.prototype.disconnect = function (callback) {

  try {

    if (this.connection) {

      this.DISCONNECTED = true;
      return this.connection.close(callback);
    }

  } catch (e) {
    console.warn('failed disconnecting mongo client', e);
  }

  callback();
};

MongoDataStore.prototype.setUpCache = function (callback) {

  var _this = this;

  var Cache = require('redis-lru-cache');

  _this.cache = new Cache(_this.config.cache);

  _this.__oldFind = _this.find;

  _this.find = function (criteria, searchOptions, sortOptions, callback) {

    if (criteria.path.indexOf && criteria.path.indexOf('*') == -1) {

      return _this.cache.get(criteria.path, function (e, item) {

        if (e) return callback(e);

        if (item) return callback(null, [item]);

        _this.__oldFind(criteria, searchOptions, sortOptions, function (e, items) {

          if (e) return callback(e);

          if (!items || items.length == 0) return callback(null, []);

          _this.cache.set(criteria.path, items[0], function (e) {

            return callback(e, items);
          });
        });
      });
    }
    return this.__oldFind(criteria, searchOptions, sortOptions, callback);
  }.bind(this);

  this.__oldFindOne = this.findOne;

  _this.findOne = function (criteria, fields, callback) {

    _this.cache.get(criteria.path, function (e, item) {

      if (e) return callback(e);
      if (item) return callback(null, item);

      _this.__oldFindOne(criteria, fields, function (e, item) {

        if (e) return callback(e);
        if (!item) return callback(null, null);

        _this.cache.set(criteria.path, item, function (e) {

          return callback(e, item);
        });
      });
    });

  }.bind(this);

  this.__oldFindAndModify = this.findAndModify;

  this.findAndModify = function (criteria, data, callback) {

    //this is actually a set or update
    _this.__oldFindAndModify(criteria, data, function (e, item) {

      if (e) return callback(e);

      if (item) return _this.cache.set(criteria.path, item, function (e) {

        callback(e, item);
      });

      callback(null, null);
    });
  };

  this.__oldRemove = this.remove;

  this.remove = function (criteria, callback) {

    if (criteria.path.indexOf && criteria.path.indexOf('*') == -1) {

      //its ok if the actual remove fails, as the cache will refresh
      _this.cache.remove(criteria.path, function (e) {

        if (e) return callback(e);

        _this.__oldRemove(criteria, callback);
      })

    } else {

      _this.find(criteria, {fields: {path: 1}}, null, function (e, items) {

        if (e) return callback(e);

        //clear the items from the cache
        async.eachSeries(items, function (item, itemCB) {

          _this.cache.remove(item.path, itemCB);

        }, function (e) {

          if (e) return callback(e);

          _this.__oldRemove(criteria, callback);
        });
      });
    }
  };

  _this.__oldInsert = _this.insert;

  _this.insert = function (data, options, callback) {

    if (typeof options == 'function') {
      callback = options;
      options = null;
    }

    if (!options) options = {};

    _this.__oldInsert(data, function (e, response) {

      if (e) return callback(e);

      if (options.noCache) return callback(null, response);

      _this.cache.set(data.path, data, function (e) {

        if (e) return callback(e);

        return callback(null, response);
      });
    });
  };

  this.__oldUpdate = this.update;

  _this.update = function (criteria, data, options, callback) {

    _this.__oldUpdate(criteria, data, options, function (e, response) {

      if (e) return callback(e);

      _this.cache.set(criteria.path, data, function (e) {

        if (e) return callback(e);

        return callback(null, response);

      });
    });
  };

  callback();
};

module.exports.create = function (config, callback) {

  try {

    var store = new MongoDataStore(config);

    store.initialize(function (e) {

      if (e) return callback(e);

      callback(null, store);
    });

  } catch (e) {
    callback(e);
  }
};