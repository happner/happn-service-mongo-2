var async = require('async');
var shortid = require('shortid');

function OperationRunner(opts){
  this.opts = opts;
}

OperationRunner.prototype.__objectFromTemplate = function(template, context){

  var newObj = {};

  for (var templatePropertyName in template){

    if (templatePropertyName.indexOf('$') == -1){
      newObj[templatePropertyName] = template[templatePropertyName];
      continue;
    }

    for (var contextPropertyName in context){
      if ('$' + contextPropertyName == templatePropertyName){
        newObj[templatePropertyName] = context[contextPropertyName];
      }
    }
  }

  return newObj;
};

OperationRunner.prototype.__getRandomPath = function(prefix){

  return prefix + shortid.generate();

};

OperationRunner.prototype.createDataPoints = function(opts, callback) {

  if (typeof opts === 'function') {
    callback = opts;
    opts = {};
  }

  if (!opts.dataStore) return callback('no dataStore passed in as option');

  if (!opts.count) opts.count = 1000;

  if (!opts.template) opts.template = {myPath:'$path', counter:'$counter'};

  if (!opts.prefix) opts.prefix = '/test/data/point/';

  if (!opts.setOptions) opts.setOptions = {};

  var dataPoints = {
    items:{},
    paths:[],
    count:0
  };

  var _this = this;

  async.times(opts.count, function(time, timeCB){

    var path = _this.__getRandomPath(opts.prefix);

    var item = _this.__objectFromTemplate(opts.template, {path:path, counter:time});

    opts.dataStore.upsert(path, item, opts.setOptions, function(e, response){

      if (e) return timeCB(e);

      if (opts.logItems) dataPoints.items[path] = {response:response};

      dataPoints.paths.push(path);

      dataPoints.count++;

      timeCB();

    });

  }, function(e){

    if (e) return callback(e);

    callback(null, dataPoints);
  });
};

OperationRunner.prototype.doGets = function(opts, callback){

  if (!opts || typeof opts === 'function' || (!opts.paths || !opts.dataStore)) return callback(new Error('you need to pass a dataStore and a list of paths you want to perform gets on'));

  if (!opts.getOptions) opts.getOptions = {};

  var asyncIterator = async.each;

  if (opts.series) asyncIterator = async.eachSeries;

  var log = {
    hits:0,
    misses:0,
    errors:0
  };

  asyncIterator(opts.paths, function(path, itemCB){

    opts.dataStore.get(path, opts.getOptions, function(e, item){

      if (e) log.errors++;

      if (item) log.hits++;
      else log.misses++;

      itemCB();

    });

  }, function(e){

    if (e) return callback(e);

    return callback(null, log);
  });

};

OperationRunner.prototype.doSets = function(opts, callback){

  if (!opts || typeof opts === 'function' || (!opts.paths || !opts.dataStore)) return callback(new Error('you need to pass a dataStore and a list of paths you want to perform sets on'));

  if (!opts.setOptions) opts.setOptions = {};

  var asyncIterator = async.each;

  if (opts.series) asyncIterator = async.eachSeries;

  var log = {
    ok:0,
    errors:0
  };

  asyncIterator(opts.paths, function(path, itemCB){

    opts.dataStore.upsert(path, opts.setData, opts.setOptions, function(e){

      if (e) log.errors++;
      else log.ok++;

      itemCB();
    });

  }, function(e){

    if (e) return callback(e);

    return callback(null, log);
  });

};

module.exports = OperationRunner;