const mongodb = require('mongodb'),
  mongoclient = mongodb.MongoClient,
  util = require('util');

MongoDataStore.prototype.ObjectID = mongodb.ObjectId;

MongoDataStore.prototype.initialize = util.promisify(initialize);
MongoDataStore.prototype.findOne = util.promisify(findOne);
MongoDataStore.prototype.count = util.promisify(count);
MongoDataStore.prototype.aggregate = util.promisify(aggregate);
MongoDataStore.prototype.find = util.promisify(find);
MongoDataStore.prototype.increment = util.promisify(increment);
MongoDataStore.prototype.insert = util.promisify(insert);
MongoDataStore.prototype.update = util.promisify(update);
MongoDataStore.prototype.findAndModify = util.promisify(findAndModify);
MongoDataStore.prototype.remove = util.promisify(remove);
MongoDataStore.prototype.disconnect = util.promisify(disconnect);

function MongoDataStore(config) {
  if (!config.database) config.database = 'happn';
  if (!config.collection) config.collection = 'happn';
  if (!config.policy) config.policy = {};
  if (!config.opts) config.opts = {};

  config.opts.useNewUrlParser = true;
  config.opts.useUnifiedTopology = true;

  Object.defineProperty(this, 'config', {
    value: config
  });
}

function initialize(callback) {
  mongoclient.connect(this.config.url, this.config.opts, (err, client) => {
    if (err) return callback(err);
    let db = client.db(this.config.database);
    let collection = db.collection(this.config.collection);

    Object.defineProperty(this, 'data', {
      value: collection
    });

    Object.defineProperty(this, 'connection', {
      value: client
    });

    return callback();
  });
}

function findOne(criteria, fields, callback) {
  return this.data.findOne(criteria, fields, callback);
}
function count(criteria, options, callback) {
  return this.data.countDocuments(criteria, options, callback);
}

function aggregate(criteria, pipeline, options, callback) {
  pipeline.unshift({ $match: criteria });
  return this.data.aggregate(pipeline, options).toArray(callback);
}

function find(criteria, searchOptions, sortOptions, callback) {
  if (searchOptions.projection) {
    //always return the path and _id for _meta
    searchOptions.projection.path = 1;
    searchOptions.projection._id = 1;
  }
  let maxTimeMS = searchOptions.maxTimeMS || 0;
  delete searchOptions.maxTimeMS;

  if (!sortOptions)
    return this.data
      .find(criteria, searchOptions)
      .maxTimeMS(maxTimeMS)
      .toArray(callback);

  this.data
    .find(criteria, searchOptions)
    .maxTimeMS(maxTimeMS)
    .sort(sortOptions)
    .toArray(callback);
}

function increment(path, counterName, increment, callback) {
  let setParameters = {
    $inc: {}
  };

  setParameters.$inc['data.' + counterName + '.value'] = increment;

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
}

function insert(data, options, callback) {
  return this.data.insertOne(data, options, callback);
}

function update(criteria, data, options, callback) {
  if (typeof options === 'function') {
    callback = options;
    options = {};
  }
  if (!options) options = {};
  options.upsert = true;
  options.returnDocument = 'after';

  return this.data.findOneAndUpdate(criteria, data, options, function(e, item) {
    if (e) {
      return callback(e);
    }
    if (item) {
      return callback(null, item.value);
    }
    callback(null, null);
  });
}

function findAndModify(criteria, data, callback) {
  return this.update(criteria, data, callback);
}

function remove(criteria, callback) {
  return this.data.deleteMany(criteria, callback);
}

function disconnect(callback) {
  try {
    if (this.connection) return this.connection.close(callback);
  } catch (e) {
    console.warn('failed disconnecting mongo client', e);
  }
  callback();
}

module.exports.create = util.promisify(function(config, callback) {
  try {
    let store = new MongoDataStore(config);

    store.initialize(function(e) {
      if (e) return callback(e);
      callback(null, store);
    });
  } catch (e) {
    callback(e);
  }
});
