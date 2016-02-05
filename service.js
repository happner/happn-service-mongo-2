var _s = require('underscore.string');
var traverse = require('traverse');
var shortid = require('shortid');

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

            db.ensureIndex('path_index', {_path:1}, {unique:true, w:1}, function(e, indexName){

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

DataMongoService.prototype.saveTag = function(path, tag, data, callback){
    var _this = this;

     var insertTag = function(snapshotData){

        var tagData = {
            data:snapshotData,

            // store out of actual address space
            _tag:tag,
            _path: '/_TAGS' + path + '/' + shortid.generate()
        }

        _this.db.insert(tagData, function(e, tag){

            if (e)
                callback(e);
            else{
                callback(null, tag);
            }


        });
     }

     if (!data){

        _this.getOneByPath(path, null, function(e, found){

            if (e)
                return callback(e);

            if (found)
            {
                data = found;
                insertTag(found);
            }
            else
                return callback('Attempt to tag something that doesn\'t exist in the first place');
        });

     }else
         insertTag(data);
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

                 //ignore _path
                if (this.key == '_path') return;

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
            dbCriteria.$and.push({"_path":{ $regex: new RegExp(path.replace(/[*]/g,'.*'))}});
        }
        else {
            dbCriteria.$and.push({"_path":path});
        }

        if (parameters.criteria){
            single = false;
            dbCriteria.$and.push(_this.parseFields(parameters.criteria));
        }

        var cursor = _this.db.find(dbCriteria, dbFields);

        if (parameters.options.sort){
            cursor = cursor.sort(_this.parseFields(parameters.options.sort));
        }

        if (parameters.options.limit)
            cursor = cursor.limit(parameters.options.limit);

        cursor.exec(function(e, items){

            if (e) return callback(e);

            if (parameters.options.path_only) {
                return callback(e, {
                    paths: items.map(function(itm) {
                        return _this.transform(itm);
                    })
                });
            }

            if (single) {
                if (!items[0]) return callback(e, null);
                return callback(e, _this.transform(items[0]));
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
        //appends an item with a path that matches the message path - but made unique by a shortid at the end of the path
        if (!_s.endsWith(path, '/'))
            path += '/';

        path += shortid.generate();

    }

    var setData = _this.formatSetData(path, data);

    if (options.tag) {
        if (data != null) {
            return callback(new Error('Cannot set tag with new data.'));
        }
        setData.data = {};
        options.merge = true;
    }

    if (options.merge){

        return _this.getOneByPath(path, null, function(e, previous){

            if (e)
                return callback(e);

            if (!previous) return _this.upsertInternal(path, setData, options, true, callback);

            for (var propertyName in previous.data)
                if (setData.data[propertyName] === null || setData.data[propertyName] === undefined)
                    setData.data[propertyName] = previous.data[propertyName];

            setData.created = previous.created;
            setData.modified = Date.now();
            setData._path = previous._path;

            _this.upsertInternal(path, setData, options, true, callback);

         });

    }

    _this.upsertInternal(path, setData, options, false, callback);

}

DataMongoService.prototype.transform = function(dataObj, additionalMeta){
    var transformed = {};

    transformed.data = dataObj.data;

    transformed._meta = {
        path:dataObj._path,
        tag:dataObj._tag
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

DataMongoService.prototype.upsertInternal =function(path, setData, options, dataWasMerged, callback){
    var _this = this;
    var setParameters = {$set: {"data":setData.data, "_path":setData._meta.path}, $setOnInsert:{"created":Date.now()}};

    console.log('have setParameters:::', setParameters);

    _this.db.update({"_path":path}, setParameters, {upsert:true}, function(err, response) {



        if (err){

            //data with circular references can cause callstack exceeded errors

            if (err.toString() == 'RangeError: Maximum call stack size exceeded')
                return callback(new Error('callstack exceeded: possible circular data in happn set method'));

            return callback(err);
        }

        var created = null;

        if (response.upserted)
            created = response.upserted[0];

        console.log('did update:::', response);

        if (dataWasMerged && !options.tag) {
            if (created) return callback(null, _this.transform(created));
            return callback(null, _this.transform(setData, setData._meta));
        }

        if (dataWasMerged && options.tag){ // we have a prefetched object, and we want to tag it

            return _this.saveTag(path, options.tag, setData, function(e, tagged){

                if (e)
                    return callback(e);

                return callback(null, _this.transform(tagged, tagged._meta));
            });
        }

        if (!dataWasMerged && !options.tag){ // no prefetched object, and we dont need to tag - we need to fetch the object

            if (created) return callback(null, _this.transform(created));

            setData._path = path;
            setData.modified = Date.now();
            callback(null, _this.transform(setData, setData._meta));

        }

    }.bind(_this));
}

DataMongoService.prototype.remove = function(path, options, callback){
    var _this = this;

    var criteria = {"_path":path};

    if (path.indexOf('*') > -1)
        criteria = {"_path":{ $regex: new RegExp(path.replace(/[*]/g,'.*'))  }};

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
