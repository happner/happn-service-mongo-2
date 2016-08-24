var _s = require('underscore.string');
var traverse = require('traverse');
var uuid = require('node-uuid');
var sift = require('sift');

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

DataMongoService.prototype.initialize = function (config, callback) {

  var _this = this;

  var Datastore = require('mongodb');

  var MongoClient = Datastore.MongoClient;

  if (!config.collection) config.collection = 'happn';

  if (!config.url) config.url = 'mongodb://127.0.0.1:27017/' + config.collection;

  MongoClient.connect(config.url, config.opts, function (err, db) {
    if (err) callback(err);
    else {

      db.ensureIndex('path_index', {path: 1}, {unique: true, w: 1}, function (e) {

        if (!e) {
          _this.config = config;
          _this.db = db.collection(config.collection);
          _this.ObjectID = Datastore.ObjectID;
          callback();
        } else
          callback(e);

      });
    }
  });
}

DataMongoService.prototype.getOneByPath = function (path, fields, callback) {
  var _this = this;

  if (!fields)
    fields = {};

  _this.db.findOne({path: path}, fields, function (e, findresult) {

    if (e)
      return callback(e);

    return callback(null, findresult);

  });
}

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

DataMongoService.prototype.get = function (path, parameters, callback) {
  var _this = this;

  try {

    if (typeof parameters == 'function') {
      callback = parameters;
      parameters = null;
    }

    if (!parameters) parameters = {};

    if (!parameters.options) parameters.options = {};

    //TODO: parameters.options.fields

    var dbFields = {};

    var pathCriteria = {"path": path};

    var single = true;

    if (parameters.options.path_only) dbFields = {_meta: 1};

    if (path.indexOf('*') >= 0) {
      single = false;
      pathCriteria = {"path": {$regex: new RegExp(path.replace(/[*]/g, '.*'))}};
    }

    if (parameters.criteria) single = false;

    var searchOptions = {};

    if (parameters.options.sort) searchOptions.sort = parameters.options.sort;

    if (parameters.options.limit) searchOptions.limit = parameters.options.limit;

    searchOptions.fields = dbFields;

    _this.db.find(pathCriteria, searchOptions).toArray(function (e, items) {

      if (e) return callback(e);

      _this.filter(parameters.criteria, items, function(e, filtered){

        if (e) return callback(e);

        if (parameters.options.path_only) {
          return callback(e, {
            paths: filtered.map(function (itm) {
              return _this.transform(itm, null, parameters.options.fields);
            })
          });
        }

        if (single) {
          if (!filtered[0]) return callback(null, null);
          return callback(null, _this.transform(filtered[0], null, parameters.options.fields));
        }

        callback(null, filtered.map(function (item) {
          return _this.transform(item, null, parameters.options.fields);
        }));

      });
    });

  } catch (e) {
    callback(e);
  }
};

// DataMongoService.prototype.get = function (path, parameters, callback) {
//   var _this = this;
//
//   try {
//
//     if (typeof parameters == 'function') {
//       callback = parameters;
//       parameters = null;
//     }
//
//     if (!parameters) parameters = {};
//
//     if (!parameters.options) parameters.options = {};
//
//     var dbFields = parameters.options.fields || {};
//     var dbCriteria = {$and: []};
//     var single = true;
//
//     if (parameters.options.path_only) {
//       dbFields = {_meta: 1}
//     } else if (parameters.options.fields) {
//       dbFields._meta = 1;
//     }
//
//     dbFields = _this.parseFields(dbFields);
//
//     if (path.indexOf('*') >= 0) {
//       single = false;
//       dbCriteria.$and.push({"path": {$regex: new RegExp(path.replace(/[*]/g, '.*'))}});
//     }
//     else {
//       dbCriteria.$and.push({"path": path});
//     }
//
//     if (parameters.criteria) {
//       single = false;
//       dbCriteria.$and.push(_this.parseFields(parameters.criteria));
//     }
//
//     var searchOptions = {};
//
//     if (parameters.options.sort) searchOptions.sort = parameters.options.sort;
//
//     if (parameters.options.limit) searchOptions.limit = parameters.options.limit;
//
//     if (Object.keys(dbFields).length > 0) searchOptions.fields = dbFields;
//
//     _this.db.find(dbCriteria, searchOptions).toArray(function (e, items) {
//
//       if (e) return callback(e);
//
//       if (parameters.options.path_only) {
//         return callback(e, {
//           paths: items.map(function (itm) {
//             return _this.transform(itm);
//           })
//         });
//       }
//
//       if (single) {
//         if (!items[0]) return callback(null, null);
//         return callback(null, _this.transform(items[0]));
//       }
//
//       callback(null, items.map(function (item) {
//         return _this.transform(item);
//       }));
//
//     });
//
//   } catch (e) {
//     callback(e);
//   }
// };


DataMongoService.prototype.formatSetData = function (path, data) {

  if (typeof data != 'object' || data instanceof Array == true || data instanceof Date == true || data == null)
    data = {value: data};

  var setData = {
    data: data,
    _meta: {
      path: path
    }
  };

  return setData;
};

DataMongoService.prototype.upsert = function (path, data, options, callback) {

  var _this = this;

  options = options ? options : {};

  if (data) delete data._meta;

  if (options.set_type == 'sibling') {
    //appends an item with a path that matches the message path - but made unique by a shortid at the end of the path
    if (!_s.endsWith(path, '/'))
      path += '/';

    path += uuid.v4().replace(/-/g, '');

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
        if (setData.data[propertyName] === null || setData.data[propertyName] === undefined)
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

  var transformed = {};

  transformed.data = dataObj.data;

  if (!meta) {
    meta = {};

    if (dataObj.created) meta.created = dataObj.created;
    if (dataObj.modified) meta.modified = dataObj.modified;
  }

  transformed._meta = meta;
  transformed._meta.path = dataObj.path;
  transformed._meta._id = dataObj._id;

  if (dataObj._tag) transformed._meta.tag = dataObj._tag;

  //strip out unwanted fields
  if (fields){
    for (var fieldName in transformed){
      if (fields[fieldName] != 1) delete transformed[fieldName];
    }
  }

  return transformed;
};

DataMongoService.prototype.formatSetData = function (path, data) {

  if (typeof data != 'object' || data instanceof Array == true || data instanceof Date == true || data == null)
    data = {value: data};

  var setData = {
    data: data,
    _meta: {
      path: path
    },
  }

  return setData;
};

DataMongoService.prototype.insertTag = function(snapshotData, tag, path, callback){

  var _this = this;

  var tagData = {
    data: snapshotData,

    // store out of actual address space
    _tag: tag,
    path: '/_TAGS' + path + '/' + uuid.v4().replace(/-/g, '')
  };

  _this.db.insert(tagData, function (e, tag) {

    if (e) return callback(e);
    callback(null, _this.transform(tag.ops[0]));
  });
};

DataMongoService.prototype.saveTag = function (path, tag, callback) {

  var _this = this;

  _this.getOneByPath(path, null, function (e, found) {

    if (e) return callback(e);

    if (!found) return callback(new Error('Attempt to tag something that doesn\'t exist in the first place'));

    _this.insertTag(_this.transform(found), tag, path, callback);

  });
};

DataMongoService.prototype.__upsertInternal = function (path, setData, options, dataWasMerged, callback) {
  var _this = this;

  var modifiedOn = Date.now();

  var setParameters = {
    $set: {"data": setData.data, "path": path, "modified": modifiedOn},
    $setOnInsert: {"created": modifiedOn}
  };

  if (options.tag) return this.saveTag(path, options.tag, callback);

  _this.db.findAndModify({"path": path}, null, setParameters, {upsert: true, "new": true}, function (err, response) {

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

  if (path.indexOf('*') > -1) criteria = {'path': {$regex: new RegExp(path.replace(/[*]/g, '.*'))}};

  this.db.remove(criteria, {multi: true}, function (err, removed) {

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

DataMongoService.prototype.addDataStoreFilter = function (pattern, datastoreKey) {
  this.log.warn('addDataStoreFilter not implemented');
};

DataMongoService.prototype.removeDataStoreFilter = function (pattern) {
  this.log.warn('removeDataStoreFilter not implemented');
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
