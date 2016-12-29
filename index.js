var _s = require('underscore.string')
  , traverse = require('traverse')
  , uuid = require('node-uuid')
  , sift = require('sift')
  , Promise = require('bluebird')
  , url = require('url')
  , async = require('async')
;

module.exports = DataMongoService;

function DataMongoService(opts) {

  var Logger;

  if (opts && opts.logger) {
    Logger = opts.logger.createLogger('DataMongo');
  } else {
    Logger = require('happn-logger');
    Logger.configure({logLevel: 'info'});
  }

  this.log = Logger.createLogger('DataMongo');
  this.log.$$TRACE('construct(%j)', opts);
}

DataMongoService.prototype.stop = function (options, callback) {

  if (typeof options === 'function')
    callback = options;

  try {
    callback();
  } catch (e) {
    callback(e);
  }
};

DataMongoService.prototype.pathField = "path";

DataMongoService.prototype.doGet = Promise.promisify(function(message, callback){

  return this.get(message.request.path, message.request.options, function(e, response){

    if (e) return callback(e);

    message.response = response;

    return callback(null, message);
  });
});

DataMongoService.prototype.doRemove = Promise.promisify(function(message, callback){

  return this.remove(message.request.path, message.request.options, function(e, response){

    if (e) return callback(e);

    message.response = response;

    return callback(null, message);
  });
});

DataMongoService.prototype.doStore = Promise.promisify(function(message, callback){


  return this.upsert(message.request.path, message.request.data, message.request.options, function(e, response){

    if (e) return callback(e);

    message.response = response;

    return callback(null, message);
  });
});

DataMongoService.prototype.doSecureStore = Promise.promisify(function(message, callback){


  if (!message.request.options) message.request.options = {};

  message.request.options.modifiedBy = message.session.user.username;

  return this.upsert(message.request.path, message.request.data, message.request.options, function(e, response){

    if (e) return callback(e);

    message.response = response;

    return callback(null, message);
  });
});

DataMongoService.prototype.doNoStore = Promise.promisify(function(message, callback){

  message.response = this.formatSetData(message.request.path, message.request.data);

  return callback(null, message);
});

DataMongoService.prototype.getDBConnectionOpts = function(connUrl, collection){

  var urlParts = url.parse(connUrl);

  var config = {};

  if (urlParts.pathname){

    var pathParts = urlParts.pathname.split('/');

    if (pathParts.length == 2 && pathParts[1] != ''){
      //we have a collection in the url
      config.url = urlParts.protocol + '//' + urlParts.host;
      config.collection = pathParts[1];
    }
  } else config.url = connUrl;

  //override
  if (collection) config.collection = collection;

  return config;
};

DataMongoService.prototype.initialize = function (config, callback) {

  var _this = this;

  var Datastore = require('mongodb');

  var MongoClient = Datastore.MongoClient;

  _this.config = config;

  _this.ObjectID = Datastore.ObjectID;

  if (!_this.config.url) _this.config.url = 'mongodb://127.0.0.1:27017';

  else {

    var defaultOpts = _this.getDBConnectionOpts(_this.config.url, _this.config.collection);

    _this.config.url = defaultOpts.url;
    _this.config.collection = defaultOpts.collection;
  }

  if (!_this.config.collection) _this.config.collection = 'happn';

  if (!config.datastores || config.datastores.length == 0) {

    config.datastores = [{
      name:'default',
      url:_this.config.url,
      collection:_this.config.collection
    },{
      name:'system',
      url:_this.config.url,
      collection:'happn-system',
      patterns:['/_SYSTEM/*']

    }];
  }

  _this.datastores = {};
  _this.dataroutes = {};

  var dataStorePos = 0;

  async.eachSeries(_this.config.datastores, function(datastoreConfig, datastoreCallback){

    if (!datastoreConfig.name) return datastoreCallback(new Error('invalid configuration, datastore config at position ' + dataStorePos + ' has no name'));

    if (dataStorePos == 0) _this.defaultDatastore = datastoreConfig.name;//just in case we havent set a default

    dataStorePos++;

    if (!datastoreConfig.collection) datastoreConfig.collection = datastoreConfig.name;

    if (!datastoreConfig.url) datastoreConfig.url = _this.config.url;

    else {

      var datastoreOpts = _this.getDBConnectionOpts(datastoreConfig.url, datastoreConfig.collection);

      datastoreConfig.url = datastoreOpts.url;
      datastoreConfig.collection = datastoreOpts.collection;

    }

    _this.datastores[datastoreConfig.name] = {};

    if (!datastoreConfig.settings) datastoreConfig.settings = {};

    if (!datastoreConfig.patterns) datastoreConfig.patterns = [];

    //make sure we match the special /_TAGS patterns to find the right db for a tag
    datastoreConfig.patterns.every(function (pattern) {

      if (pattern.indexOf('/') == 0) pattern = pattern.substring(1, pattern.length);

      _this.addDataStoreFilter(pattern, datastoreConfig.name);

      return true;
    });

    //forces the default datastore
    if (datastoreConfig.isDefault) _this.defaultDatastore = datastoreConfig.name;

    console.log('ds filter:::', datastoreConfig.url + '/' + datastoreConfig.collection);

    MongoClient.connect (datastoreConfig.url + '/' + datastoreConfig.collection, datastoreConfig.opts, function (err, db) {

      if (err) datastoreCallback(err);

      else {

        db.ensureIndex('path_index', {path: 1}, {unique: true, w: 1}, function (e) {

          if (!e) {

            _this.datastores[datastoreConfig.name].config = datastoreConfig;
            _this.datastores[datastoreConfig.name].db = db.collection(datastoreConfig.collection);

            datastoreCallback();

          } else datastoreCallback(e);
        });
      }
    });

  }, function(e){

    if (e) return callback(e);

    _this.db = function (path) {

      for (var dataStoreRoute in _this.dataroutes) if (_this.happn.services.utils.wildcardMatch(dataStoreRoute, path)) return _this.dataroutes[dataStoreRoute].db;

      console.log('laoding default ds:::');
      console.log(_this.defaultDatastore);
      console.log(_this.datastores);

      return _this.datastores[_this.defaultDatastore].db;
    };

    callback();

  });
};

DataMongoService.prototype.addDataStoreFilter = function (pattern, datastoreKey) {

  if (!datastoreKey) throw new Error('missing datastoreKey parameter');

  var dataStore = this.datastores[datastoreKey];

  if (!dataStore) throw new Error('no datastore with the key ' + datastoreKey + ', exists');

  var tagPattern = pattern.toString();

  if (tagPattern.indexOf('/') == 0) tagPattern = tagPattern.substring(1, tagPattern.length);

  this.dataroutes[pattern] = dataStore;
  this.dataroutes['/_TAGS/' + tagPattern] = dataStore;
};

DataMongoService.prototype.removeDataStoreFilter = function (pattern) {

  var tagPattern = pattern.toString();

  if (tagPattern.indexOf('/') == 0)
    tagPattern = tagPattern.substring(1, tagPattern.length);

  delete this.dataroutes[pattern];
  delete this.dataroutes['/_TAGS/' + tagPattern];

};

DataMongoService.prototype.getOneByPath = function (path, fields, callback) {
  var _this = this;

  if (!fields)
    fields = {};

  _this.db(path).findOne({path: path}, fields, function (e, findresult) {

    if (e)
      return callback(e);

    return callback(null, findresult);

  });
};

DataMongoService.prototype.randomId = function(){
  return Date.now() + '_' + uuid.v4().replace(/-/g, '');
};

DataMongoService.prototype.insertTag = function(snapshotData, tag, path, callback){

  var _this = this;

  var baseTagPath = '/_TAGS';

  if (path.substring(0, 1) != '/') baseTagPath += '/';

  var tagPath = baseTagPath + path + '/' + _this.randomId();

  var tagData = {

    data: snapshotData,

    _tag: tag,

    path: tagPath
  };

  this.__upsertInternal(tagPath, tagData, {}, false, callback);

};

DataMongoService.prototype.saveTag = function (path, tag, data, callback) {

  if (!data) {

    this.getOneByPath(path, null, function (e, found) {

      if (e) return callback(e);

      if (found) {
        data = found;
        this.insertTag(found, tag, path, callback);
      }
      else return callback(new Error('Attempt to tag something that doesn\'t exist in the first place'));

    });

  } else this.insertTag(data, tag, path, callback);
};

DataMongoService.prototype.parseFields = function (fields) {

  traverse(fields).forEach(function (value) {

    if (value) {

      if (value.bsonid) this.update(value.bsonid);

      //ignore elements in arrays
      if (this.parent && Array.isArray(this.parent.node)) return;

      if (typeof this.key == 'string') {

        //ignore directives
        if (this.key.indexOf('$') == 0) return;

        //ignore _meta
        if (this.key == '_meta') return;

        //ignore _id
        if (this.key == '_id') return;

        //ignore path
        if (this.key == 'path') return;

        //look in the right place for created
        if (this.key == '_meta.created') {
          fields['created'] = value;
          return this.remove();
        }

        //look in the right place for modified
        if (this.key == '_meta.modified') {
          fields['modified'] = value;
          return this.remove();
        }

        //prepend with data.
        fields['data.' + this.key] = value;
        return this.remove();

      }
    }
  });

  return fields;
};

DataMongoService.prototype.filter = function(criteria, data, callback){

  if (!criteria) return callback(null, data);

  try{

    var filterCriteria = this.parseFields(criteria);

    callback(null, sift(filterCriteria, data));
  }catch(e){
    callback(e);
  }

};

DataMongoService.prototype.__doFind = function(path, searchOptions, sortOptions, callback){

  var _this = this;

  var db = _this.db(path);

  var pathCriteria = _this.getPathCriteria(path);

  if (!sortOptions) db.find(pathCriteria, searchOptions).toArray(callback);

  else {

    sortOptions = _this.parseFields(sortOptions);
    db.find(pathCriteria, searchOptions).sort(sortOptions).toArray(callback);
  }
};

DataMongoService.prototype.getPathCriteria = function(path){

  var dbCriteria = {$and: []};

  var returnType = path.indexOf('*'); //0,1 == array -1 == single

  if (returnType == 0) dbCriteria.$and.push({'path': {$regex: new RegExp(path.replace(/[*]/g, '.*'))}});//keys with any prefix ie. */joe/bloggs

  else if (returnType > 0) dbCriteria.$and.push({'path': {$regex: new RegExp('^' + path.replace(/[*]/g, '.*'))}});//keys that start with something but any suffix /joe/*/bloggs/*

  else dbCriteria.$and.push({'path': path}); //precise match

  return dbCriteria;
};

DataMongoService.prototype.get = function (path, parameters, callback) {

  var _this = this;

  try {

    if (typeof parameters == 'function') {
      callback = parameters;
      parameters = null;
    }

    var dbFields = {};

    if (!parameters) parameters = {options:{}};

    else {

      if (!parameters.options) parameters.options = {};

      else {

        if (parameters.options.path_only) dbFields = {_meta: 1};

        else if (parameters.options.fields){
          dbFields = parameters.options.fields;
          dbFields._meta = 1;
        }
      }
    }

    dbFields = _this.parseFields(dbFields);

    var searchOptions = {fields:dbFields};

    _this.__doFind(path, searchOptions, parameters.options.sort, function (e, items) {

      if (e) return callback(e);

      if (path.indexOf('*') == -1) {//this is a single item
        if (items.length == 0) return callback(null, null);
        return callback(null, _this.transform(items[0], null, parameters.options.fields));
      }

      _this.filter(parameters.criteria, items, function(e, filtered){

        if (e) return callback(e);

        if (parameters.options.limit) filtered = filtered.slice(0, parameters.options.limit);

        if (parameters.options.path_only) {
          return callback(e, {
            paths: _this.transformAll(filtered)
          });
        }

        callback(null, _this.transformAll(filtered, parameters.options.fields));
      });
    });

  } catch (e) {
    callback(e);
  }
};

DataMongoService.prototype.upsert = function (path, data, options, callback) {

  var _this = this;

  if (typeof options === 'function'){
    callback = options;
    options = null;
  }

  options = options ? options : {};

  if (data) delete data._meta;

  if (options.set_type == 'sibling') {
    //appends an item with a path that matches the message path - but made unique by a shortid at the end of the path
    if (!_s.endsWith(path, '/')) path += '/';

    path += _this.randomId();;
  }

  var setData = _this.formatSetData(path, data);

  if (options.tag) {

    if (data != null) return callback(new Error('Cannot set tag with new data.'));

    setData.data = {};
    options.merge = true;
  }

  if (options.merge) {

    return _this.getOneByPath(path, null, function (e, previous) {

      if (e) return callback(e);

      if (!previous) return _this.__upsertInternal(path, setData, options, true, callback);

      for (var propertyName in previous.data)
        if (setData.data[propertyName] == null)
          setData.data[propertyName] = previous.data[propertyName];

      setData.created = previous.created;
      setData.modified = Date.now();
      setData._id = previous._id;

      _this.__upsertInternal(path, setData, options, true, callback);
    });
  }

  _this.__upsertInternal(path, setData, options, false, callback);
};

DataMongoService.prototype.transform = function (dataObj, meta, fields) {

  var transformed = {data:dataObj.data};

  if (!meta) {

    meta = {};

    if (dataObj.created) meta.created = dataObj.created;

    if (dataObj.modified) meta.modified = dataObj.modified;

    if (dataObj.modifiedBy) meta.modifiedBy = dataObj.modifiedBy;
  }

  transformed._meta = meta;
  transformed._meta.path = dataObj.path;
  transformed._meta._id = dataObj.path;

  if (dataObj._tag) transformed._meta.tag = dataObj._tag;

  if (fields) for (var fieldName in transformed) if (fields[fieldName] != 1) delete transformed[fieldName];

  return transformed;
};

DataMongoService.prototype.transformAll = function (items, fields) {

  var _this = this;

  return items.map(function (item) {
    return _this.transform(item, null, fields);
  })
};

DataMongoService.prototype.formatSetData = function (path, data, options) {

  if (typeof data != 'object' ||
    data instanceof Array == true ||
    data instanceof Date == true ||
    data == null)

    data = {value: data};

  if (options && options.modifiedBy)
    return {
      data: data,
      _meta: {
        path: path,
        modifiedBy:options.modifiedBy
      }
    };

  return {
    data: data,
    _meta: {
      path: path
    }
  };
};

DataMongoService.prototype.__upsertInternal = function (path, setData, options, dataWasMerged, callback) {
  var _this = this;

  var modifiedOn = Date.now();

  var setParameters = {
    $set: {"data": setData.data, "path": path, "modified": modifiedOn},
    $setOnInsert: {"created": modifiedOn}
  };

  if (options.tag) return this.saveTag(path, options.tag, setData, callback);

  if (setData._tag) setParameters.$set._tag = setData._tag;

  if (setData._meta && setData._meta.modifiedBy) setParameters.$set.modifiedBy = setData._meta.modifiedBy;

  _this.db(path).findAndModify({"path": path}, null, setParameters, {upsert: true, "new": true}, function (err, response) {

    if (err) {
      //data with circular references can cause callstack exceeded errors
      if (err.toString() == 'RangeError: Maximum call stack size exceeded') return callback(new Error('callstack exceeded: possible circular data in happn set method'));

      return callback(err);
    }

    callback(null, _this.transform(response.value));

  }.bind(_this));
};

DataMongoService.prototype.remove = function (path, options, callback) {
  var criteria = {'path': path};

  this.db(path).remove(this.getPathCriteria(path), {multi: true}, function (err, removed) {

    if (err) return callback(err);

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

DataMongoService.prototype.compact = function (dataStoreKey, callback) {
  this.log.warn('compact not implemented');
  callback();
};

DataMongoService.prototype.startCompacting = function (dataStoreKey, interval, callback, compactionHandler) {
  this.log.warn('startCompacting not implemented');
  callback();
};

DataMongoService.prototype.stopCompacting = function (dataStoreKey, callback) {
  this.log.warn('stopCompacting not implemented');
  callback();
};
//stopCompacting
