var _s = require('underscore.string');
var traverse = require('traverse');
var uuid = require('node-uuid');

module.exports = DataMongoService;

function DataMongoService(opts) {

    var Logger;

    if (opts && opts.logger){
        Logger = opts.logger.createLogger('DataMongo');
    }else{
        Logger = require('happn-logger');
        Logger.configure({logLevel: 'info'});
    }

    this.log = Logger.createLogger('DataMongo');
    this.log.$$TRACE('construct(%j)', opts);
}

DataMongoService.prototype.stop = function(options, done){
    var _this = this;

    if (typeof options == 'function')
        done = options;

    try{
        done();
    }catch(e){
        done(e);
    }
}

DataMongoService.prototype.initialize = function(config, done){

    var _this = this;

    var Datastore = require('mongodb');
    var MongoClient = Datastore.MongoClient;

    if (!config.collection)
        config.collection = 'happn';

    if (!config.url)
        config.url = 'mongodb://127.0.0.1:27017/happn';

     MongoClient.connect(config.url, config.opts, function(err, db) {
        if(err) done(err);
        else{

            db.ensureIndex('path_index', {path:1}, {unique:true, w:1}, function(e, indexName){

                if (!e){
                    _this.config = config;
                    _this.db = db.collection(config.collection);
                    _this.ObjectID = Datastore.ObjectID;
                    done();
                }else
                    done(e);

            });
        }
    });
}

DataMongoService.prototype.getOneByPath = function(path, fields, callback){
     var _this = this;

     if (!fields)
        fields = {};

     _this.db.findOne({ path: path }, fields, function(e, findresult){

        if (e)
            return callback(e);

        return callback(null, findresult);

     });
}

DataMongoService.prototype.parseFields = function(fields){

    var _this = this;
    traverse(fields).forEach(function (value) {

        if (value){

            if (value.bsonid) this.update(value.bsonid);

            //ignore elements in arrays
            if (this.parent && Array.isArray(this.parent.node)) return;

            if (typeof this.key == 'string'){

                //ignore directives
                if (this.key.indexOf('$') == 0) return;

                //ignore _meta
                if (this.key == '_meta') return;

                //ignore _id
                if (this.key == '_id') return;

                 //ignore path
                if (this.key == 'path') return;

                //look in the right place for created
                if (this.key == '_meta.created'){
                   fields['created'] = value;
                   return this.remove();
                }

                //look in the right place for modified
                if (this.key == '_meta.modified'){
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
}

DataMongoService.prototype.get = function(path, parameters, callback){
    var _this = this;

     try{

        if (typeof parameters == 'function'){
            callback = parameters;
            parameters = null;
        }

        if (!parameters)
            parameters = {};

        if (!parameters.options)
            parameters.options = {};

        var dbFields = parameters.options.fields || {};
        var dbCriteria = {$and:[]};
        var single = true;

        if (parameters.options.path_only) {
           dbFields = {_meta:1}
        }else if (parameters.options.fields) {
           dbFields._meta = 1;
        }

        dbFields = _this.parseFields(dbFields);

        if (path.indexOf('*') >= 0) {
            single = false;
            dbCriteria.$and.push({"path":{ $regex: new RegExp(path.replace(/[*]/g,'.*'))}});
        }
        else {
            dbCriteria.$and.push({"path":path});
        }

        if (parameters.criteria){
            single = false;
            dbCriteria.$and.push(_this.parseFields(parameters.criteria));
        }

        var searchOptions = {};

        if (parameters.options.sort)
           searchOptions.sort = parameters.options.sort;

        if (parameters.options.limit)
            searchOptions.limit = parameters.options.limit;

        _this.db.find(dbCriteria, searchOptions).toArray(function(e, items){

            if (parameters.options.path_only) {
                return callback(e, {
                    paths: items.map(function(itm) {
                        return _this.transform(itm);
                    })
                });
            }

            if (single) {
                if (!items[0]) return callback(null, null);
                return callback(null, _this.transform(items[0]));
            }

            callback(null, items.map(function(item) {
                return _this.transform(item);
            }));


        });


    }catch(e){
        callback(e);
    }
}

DataMongoService.prototype.formatSetData = function(path, data){

    if (typeof data != 'object' || data instanceof Array == true || data instanceof Date == true || data == null)
        data = {value:data};

    var setData = {
        data: data,
        _meta: {
            path: path
        },
    }

    return setData;
}

DataMongoService.prototype.upsert = function(path, data, options, callback){
     var _this = this;

    options = options?options:{};

    if (data) delete data._meta;

    if (options.set_type == 'sibling'){
        //appends an item with a path that matches the message path - but made unique by a uuid at the end of the path
        if (!_s.endsWith(path, '/'))
            path += '/';

        path += uuid.v4();

    }

    var setData = _this.formatSetData(path, data);

    if (options.tag) options.merge = true;

    if (options.merge){

        return _this.getOneByPath(path, null, function(e, previous){

            if (e) return callback(e);

            if (options.tag){

                if (!previous) return callback(new Error('attempt to tag non-existent data'));
                _this.upsertInternal('/_TAGS' + previous.path + '/' + uuid.v4(), {data:previous}, {tag:options.tag}, callback);

            }else{

                for (var propertyName in previous.data)
                    if (setData.data[propertyName] === null || setData.data[propertyName] === undefined)
                        setData.data[propertyName] = previous.data[propertyName];

                _this.upsertInternal(path, setData, options, callback);
            }

         });

    }

    _this.upsertInternal(path, setData, options, callback);

}

DataMongoService.prototype.transform = function(dataObj, additionalMeta){
    var transformed = {};

    transformed.data = dataObj.data;

    transformed._meta = {
        path:dataObj.path,
        tag:dataObj.tag
    }

    if (dataObj.created)
        transformed._meta.created = dataObj.created;

    if (dataObj.modified)
        transformed._meta.modified = dataObj.modified;

    if (additionalMeta){
        for (var additionalProperty in additionalMeta)
           transformed._meta[additionalProperty] = additionalMeta[additionalProperty];
    }

    return transformed;
}

DataMongoService.prototype.formatSetData = function(path, data){

    if (typeof data != 'object' || data instanceof Array == true || data instanceof Date == true || data == null)
        data = {value:data};

    var setData = {
        data: data,
        _meta: {
            path: path
        },
    }

    return setData;
}

DataMongoService.prototype.upsertInternal =function(path, setData, options, callback){
    var _this = this;

    var modifiedOn = Date.now();
    var setParameters = {$set: {"data":setData.data, "path":path, "modified":modifiedOn}, $setOnInsert:{"created":modifiedOn}};

    if (options.tag)
        setParameters.$set.tag = options.tag;

    _this.db.findAndModify({"path":path}, null, setParameters, {upsert:true, "new":true}, function(err, response) {

        if (err){
            //data with circular references can cause callstack exceeded errors
            if (err.toString() == 'RangeError: Maximum call stack size exceeded')
                return callback(new Error('callstack exceeded: possible circular data in happn set method'));

            return callback(err);
        }

        callback(null, _this.transform(response.value));

    }.bind(_this));
}

DataMongoService.prototype.remove = function(path, options, callback){
    var _this = this;

    var criteria = {"path":path};

    if (path.indexOf('*') > -1)
        criteria = {"path":{ $regex: new RegExp(path.replace(/[*]/g,'.*'))  }};

    _this.db.remove(criteria, { multi: true }, function(err, removed){

        if (err) return callback(err);

        callback(null, {
            "data": {
                removed: removed
            },
            "_meta":{
                timestamp:Date.now(),
                path: path
            }
        });
    });

}
