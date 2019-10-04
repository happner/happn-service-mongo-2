const mongodb = require("mongodb"),
  mongoclient = mongodb.MongoClient,
  async = require("async");

function MongoDataStore(config) {
  if (!config.database) config.database = "happn";
  if (!config.collection) config.collection = "happn";
  if (!config.policy) config.policy = {};
  if (!config.opts) config.opts = {};

  config.opts.useNewUrlParser = true;
  config.opts.useUnifiedTopology = true;

  if (config.cache) {
    if (config.cache === true) config.cache = {};
    if (!config.cache.cacheId) config.cache.cacheId = config.name;
  }

  Object.defineProperty(this, "config", {
    value: config
  });
}

MongoDataStore.prototype.setUpCache = function(callback) {
  let _this = this;

  let Cache = require("redis-lru-cache");

  let cache = new Cache(_this.config.cache);

  Object.defineProperty(this, "cache", {
    value: cache
  });

  _this.__oldFind = _this.find;

  _this.find = function(criteria, searchOptions, sortOptions, callback) {
    if (criteria.path.indexOf && criteria.path.indexOf("*") == -1) {
      return _this.cache.get(criteria.path, function(e, item) {
        if (e) return callback(e);

        if (item) return callback(null, [item]);

        _this.__oldFind(criteria, searchOptions, sortOptions, function(
          e,
          items
        ) {
          if (e) return callback(e);

          if (!items || items.length == 0) return callback(null, []);

          _this.cache.set(criteria.path, items[0], function(e) {
            return callback(e, items);
          });
        });
      });
    }
    return this.__oldFind(criteria, searchOptions, sortOptions, callback);
  }.bind(this);

  this.__oldFindOne = this.findOne;

  _this.findOne = function(criteria, fields, callback) {
    _this.cache.get(criteria.path, function(e, item) {
      if (e) return callback(e);
      if (item) return callback(null, item);

      _this.__oldFindOne(criteria, fields, function(e, item) {
        if (e) return callback(e);
        if (!item) return callback(null, null);

        _this.cache.set(criteria.path, item, function(e) {
          return callback(e, item);
        });
      });
    });
  }.bind(this);

  this.__oldFindAndModify = this.findAndModify;

  this.findAndModify = function(criteria, data, callback) {
    //this is actually a set or update
    _this.__oldFindAndModify(criteria, data, function(e, item) {
      if (e) return callback(e);

      if (item)
        return _this.cache.set(criteria.path, item, function(e) {
          callback(e, item);
        });

      callback(null, null);
    });
  };

  this.__oldRemove = this.remove;

  this.remove = function(criteria, callback) {
    if (criteria.path.indexOf && criteria.path.indexOf("*") == -1) {
      //its ok if the actual remove fails, as the cache will refresh
      _this.cache.remove(criteria.path, function(e) {
        if (e) return callback(e);

        _this.__oldRemove(criteria, callback);
      });
    } else {
      _this.find(
        criteria,
        {
          fields: {
            path: 1
          }
        },
        null,
        function(e, items) {
          if (e) return callback(e);

          //clear the items from the cache
          async.eachSeries(
            items,
            function(item, itemCB) {
              _this.cache.remove(item.path, itemCB);
            },
            function(e) {
              if (e) return callback(e);

              _this.__oldRemove(criteria, callback);
            }
          );
        }
      );
    }
  };

  _this.__oldInsert = _this.insert;

  _this.insert = function(data, options, callback) {
    if (typeof options == "function") {
      callback = options;
      options = null;
    }

    if (!options) options = {};

    _this.__oldInsert(data, function(e, response) {
      if (e) return callback(e);

      if (options.noCache) return callback(null, response);

      _this.cache.set(data.path, data, function(e) {
        if (e) return callback(e);

        return callback(null, response);
      });
    });
  };

  this.__oldUpdate = this.update;

  _this.update = function(criteria, data, options, callback) {
    _this.__oldUpdate(criteria, data, options, function(e, response) {
      if (e) return callback(e);

      _this.cache.set(criteria.path, data, function(e) {
        if (e) return callback(e);

        return callback(null, response);
      });
    });
  };

  callback();
};

MongoDataStore.prototype.ObjectID = mongodb.ObjectID;

MongoDataStore.prototype.initialize = function(callback) {
  mongoclient.connect(this.config.url, this.config.opts, (err, client) => {
    if (err) return callback(err);
    let db = client.db(this.config.database);
    let collection = db.collection(this.config.collection);

    Object.defineProperty(this, "data", {
      value: collection
    });

    Object.defineProperty(this, "connection", {
      value: client
    });

    if (!this.config.cache) return callback();
    this.setUpCache(callback);
  });
};

MongoDataStore.prototype.findOne = function(criteria, fields, callback) {
  return this.data.findOne(criteria, fields, callback);
};

MongoDataStore.prototype.__extractNot = function(criteria) {
  if (!criteria) return null;

  let separated = {
    cleaned: {
      $and: []
    },
    nots: {
      $and: []
    }
  };

  criteria.$and.forEach(function(criterion) {
    if (criterion.$not) {
      separated.nots.$and.push(criterion);
      separated.hasNots = true;
    } else separated.cleaned.$and.push(criterion);
  });

  return separated;
};

MongoDataStore.prototype.count = function(criteria, options, callback) {
  return this.data.countDocuments(criteria, options, callback);
};

MongoDataStore.prototype.aggregate = function(
  criteria,
  pipeline,
  options,
  callback
) {
  pipeline.unshift({ $match: criteria });
  return this.data.aggregate(pipeline, options).toArray(callback);
};

MongoDataStore.prototype.find = function(
  criteria,
  searchOptions,
  sortOptions,
  callback
) {
  //this is because the $not keyword works in nedb and sift, but not in mongo
  if (searchOptions.projection) {
    //always return the path and _id for _meta
    searchOptions.projection.path = 1;
    searchOptions.projection._id = 1;
  }

  if (!sortOptions)
    return this.data.find(criteria, searchOptions).toArray(callback);

  this.data
    .find(criteria, searchOptions)
    .sort(sortOptions)
    .toArray(callback);
};

MongoDataStore.prototype.increment = function(
  path,
  counterName,
  increment,
  callback
) {
  let setParameters = {
    $inc: {}
  };

  setParameters.$inc["data." + counterName + ".value"] = increment;

  this.update(
    {
      path: path
    },
    setParameters,
    (e, updated) => {
      if (e) return callback(e);
      callback(null, updated.data[counterName].value);
    }
  );
};

MongoDataStore.prototype.insert = function(data, options, callback) {
  return this.data.insertOne(data, options, callback);
};

MongoDataStore.prototype.update = function(criteria, data, options, callback) {
  if (typeof options == "function") {
    callback = options;
    options = {};
  }
  if (!options) options = {};
  options.upsert = true;
  options.returnOriginal = false; //return the updated data
  return this.data.findOneAndUpdate(criteria, data, options, function(e, item) {
    if (e) return callback(e);
    if (item) return callback(null, item.value);
    callback(null, null);
  });
};

MongoDataStore.prototype.findAndModify = function(criteria, data, callback) {
  return this.update(criteria, data, callback);
};

MongoDataStore.prototype.remove = function(criteria, callback) {
  return this.data.deleteMany(criteria, callback);
};

MongoDataStore.prototype.disconnect = function(callback) {
  try {
    if (this.connection) return this.connection.close(callback);
  } catch (e) {
    console.warn("failed disconnecting mongo client", e);
  }
  callback();
};

module.exports.create = function(config, callback) {
  try {
    let store = new MongoDataStore(config);

    store.initialize(function(e) {
      if (e) return callback(e);
      callback(null, store);
    });
  } catch (e) {
    callback(e);
  }
};
