var shortid = require('shortid')
  , EventEmitter = require('events').EventEmitter
  , async = require('async')
  , DataStoreService = require('../../index.js')
  ;

function RandomClient(opts){

  var _this = this;

  _this.opts = opts;

  _this.setActivityTotals = {};

  _this.setActivityLogs = {};

  _this.getActivityTotals = {};

  _this.getActivityLogs = {};

  _this.__dataStore = new DataStoreService();

  _this.__eventEmitter = new EventEmitter();
}

RandomClient.prototype.initialize = function(config, callback){

  this.__dataStore.happn = {
    services:{
      utils:{
        wildcardMatch:function (pattern, matchTo) {

          var regex = new RegExp(pattern.replace(/[*]/g, '.*'));
          var matchResult = matchTo.match(regex);

          if (matchResult) return true;

          return false;
        }
      }
    }
  };

  this.__dataStore.initialize(config, callback);
};

RandomClient.prototype.__emit = function (path, data) {
  return this.__eventEmitter.emit(path, data);
};

RandomClient.prototype.on = function (path, handler) {
  return this.__eventEmitter.on(path, handler);
};

RandomClient.prototype.off = RandomClient.prototype.removeListener = function (path, handler) {
  return this.__eventEmitter.removeListener(path, handler);
};

RandomClient.prototype.dataStore = function(){
  return this.__dataStore;
};

RandomClient.prototype.deleteAll = function(items, callback){

  var _this = this;

  //console.log('IN DELETE ALL:::', items);

  async.each(items, function(item, itemCB){

    //console.log('IN DELETE ALL ITEM:::', item.path);

    _this.__dataStore.remove(item.path, {}, function(e){

      //console.log('REMOVED:::', item.path);

      if (e) return itemCB(e);

      itemCB();
    });

  }, callback);
};

RandomClient.prototype.startSetActivitySequential = function(opts, callback){

  var _this = this;

  if (opts.log) this.setActivityLogs[opts.testId] = [];

  if (_this.setActivityIsRunning) throw new Error('Set activity is already happening');

  if (!opts.testId) opts.testId = Date.now() + '_' + shortid.generate();

  if (!opts.setOptions) opts.setOptions = {};

  _this.setActivityIsRunning = true;

  var ERRCOUNTER = 0;
  var SETCOUNTER = 0;
  var TRIEDCOUNTER = 0;
  var STARTED = Date.now();
  var ENDED;
  var DURATION;

  async.timesSeries(opts.limit, function(time, timeCB){

    var path = opts.testId + '/' + time.toString();
    var value = {"test":time};

    _this.__dataStore.upsert(path, value, opts.setOptions, function(e){

      TRIEDCOUNTER++;

      if (e) ERRCOUNTER ++;

      else SETCOUNTER ++;

      if (opts.log) _this.setActivityLogs[opts.testId].push({path:path, value:value, error:e});

      SETCOUNTER++;

      timeCB();
    });

  }, function(e){

    ENDED = Date.now();

    if (e) return callback(e);

    DURATION = ENDED - STARTED;

    _this.setActivityTotals[opts.testId] = {
      testId:opts.testId,
      started:STARTED,
      ended:ENDED,
      attempts:TRIEDCOUNTER,
      sets:SETCOUNTER,
      duration:DURATION,
      errors:ERRCOUNTER
    };

    callback(null, _this.setActivityTotals[opts.testId], _this.setActivityLogs[opts.testId]);

  });
};

RandomClient.prototype.startSetActivity = function(opts){

  var _this = this;

  if (opts.log) this.setActivityLogs[opts.testId] = [];

  if (_this.setActivityIsRunning) throw new Error('Set activity is already happening');

  if (!opts.testId) opts.testId = Date.now() + '_' + shortid.generate();

  _this.setActivityIsRunning = true;

  if (!opts.setOptions) opts.setOptions = {};

  var SETCOUNTER = 0;
  var ERRCOUNTER = 0;
  var TRIEDCOUNTER = 0;

  var STARTED = Date.now();
  var ENDED;
  var DURATION;

  var doSet = function(){

    TRIEDCOUNTER ++;

    var path = opts.testId + '/' + SETCOUNTER.toString();
    var value = {"test":SETCOUNTER};

    _this.__dataStore.upsert(path, value, opts.setOptions, function(e){

      if (e) {
        if (opts.log) _this.setActivityLogs[opts.testId].push({path:path, value:value, error:e});
        return ERRCOUNTER ++;
      }

      if (opts.log) _this.setActivityLogs[opts.testId].push({path:path, value:value});

      SETCOUNTER++;

      if (opts.limit && opts.limit <= SETCOUNTER) _this.setActivityIsRunning = false;

    });

    if (!_this.setActivityIsRunning) {

      ENDED = Date.now();
      DURATION = ENDED - STARTED;

      //console.log('did ' + SETCOUNTER + ' cache sets out of ' + TRIEDCOUNTER + ' attempts in ' + DURATION + ' milliseconds with ' + ERRCOUNTER + ' FAILURES');

      _this.setActivityTotals[opts.testId] = {
        testId:opts.testId,
        started:STARTED,
        ended:ENDED,
        attempts:TRIEDCOUNTER,
        sets:SETCOUNTER,
        duration:DURATION,
        errors:ERRCOUNTER
      };

      _this.__emit('set-activity-run-complete', {totals:_this.setActivityTotals[opts.testId], logs:_this.setActivityLogs[opts.testId]});

    } else setImmediate(doSet);
  };

  doSet();

  return {
    testId:opts.testId,
    stop:function(){this.setActivityIsRunning = false;}.bind(_this)
  };
};

RandomClient.prototype.__getRandomLogItems = function(logs, amount, exclude){

  var randomItems = [];

  var usedAlready = {};

  var excludeList = {};

  if (exclude){
    exclude.forEach(function(item){
      //console.log('excludeItem:::', item.path);
      excludeList[item.path] = true;
    });

  }

  var getRandomIndex = function(){

    var foundIndex = -1;

    while(foundIndex == -1){

      var checkIndex = Math.floor(Math.random() * (logs.length - 1)) + 1;

      var checkItem = logs[checkIndex];

      //console.log('checkItem:::', excludeList[checkItem.path] == null, usedAlready[checkIndex]);

      if (!usedAlready[checkIndex] && excludeList[checkItem.path] == null){

        //console.log('OK:::');

        if (logs[checkIndex] == null) {
          //console.log('dodge, no log item at index ' + checkIndex);
          continue;
        }

        foundIndex = checkIndex;
        usedAlready[foundIndex] = true;
      }
    }
    return foundIndex;
  };

  for (var i = 0; i < amount; i ++){
    randomItems.push(logs[getRandomIndex()]);
  }

  return randomItems;
};

RandomClient.prototype.startGetActivity = function(opts) {

  var _this = this;

  if (_this.getActivityIsRunning) throw new Error('Get activity is already happening');

  if (!opts) opts = {};

  if (!opts.testId) opts.testId = Date.now() + '_' + shortid.generate();

  if (opts.log) _this.getActivityLogs[opts.testId] = {
    hits:[],
    misses:[],
    attempts:[],
    errors:[]
  };

  if (opts.limit) {
    //tries means we limit at the amount of calls made, gets is where we stop after a maximum of successful sets is reached
    if (!opts.limitMode) opts.limitMode = 'gets';
    if (['gets', 'tries'].indexOf(opts.limitMode) == -1) throw new Error('limitMode must be set and either "tries" or "gets"');
  }

  var consistencyItems;

  var delConsistencyItems;

  var doGetActivity = function () {

    _this.getActivityIsRunning = true;

    var GETCOUNTER = 0;
    var ERRCOUNTER = 0;
    var HITCOUNTER = 0;
    var MISSCOUNTER = 0;
    var TRIEDCOUNTER = 0;
    var STARTED = Date.now();
    var ENDED;
    var DURATION;

    var doGet = function () {

      TRIEDCOUNTER++;

      if (opts.limit && opts.limitMode == 'tries' && opts.limit <= TRIEDCOUNTER) if (_this.getActivityIsRunning) _this.getActivityIsRunning = false;

      if (opts.limit && opts.limitMode == 'gets' && opts.limit <= GETCOUNTER) if (_this.getActivityIsRunning) _this.getActivityIsRunning = false;

      if (_this.getActivityIsRunning){

        var path = opts.testId + '/' + GETCOUNTER.toString();

        _this.__dataStore.get(path, function (e, value) {

          if (e) {
            if (opts.log) _this.getActivityLogs[opts.testId].errors.push({path: path, value: value, error: e});
            ERRCOUNTER++;
          } else {
            if (opts.log) _this.getActivityLogs[opts.testId].attempts.push({path: path, value: value});
          }

          if (value) {
            HITCOUNTER++;
            if (opts.log) _this.getActivityLogs[opts.testId].hits.push(path);
          }
          else {
            MISSCOUNTER++;
            if (opts.log) _this.getActivityLogs[opts.testId].misses.push(path);
          }

          GETCOUNTER++;
        });

        setImmediate(doGet);

      } else {

        ENDED = Date.now();
        DURATION = ENDED - STARTED;

        //console.log('did ' + GETCOUNTER + ' cache gets out of ' + (TRIEDCOUNTER - 1) + ' attempts in ' + DURATION + ' milliseconds with ' + ERRCOUNTER + ' FAILURES');

        _this.getActivityTotals[opts.testId] = {
          testId: opts.testId,
          started: STARTED,
          ended: ENDED,
          attempts: TRIEDCOUNTER,
          gets: GETCOUNTER,
          duration: DURATION,
          errors: ERRCOUNTER,
          hits:HITCOUNTER,
          misses:MISSCOUNTER,
          consistency:consistencyItems,
          delConsistency:delConsistencyItems
        };

        var completeMessage = {
          totals:_this.getActivityTotals[opts.testId],
          logs:_this.getActivityLogs[opts.testId]
        };

        if (consistencyItems) completeMessage.consistency = consistencyItems;

        if (delConsistencyItems) completeMessage.delConsistency = delConsistencyItems;

        _this.__emit('get-activity-run-complete', completeMessage);
      }
    };

    doGet();

    return {
      testId: opts.testId,
      stop: function () {
        _this.getActivityIsRunning = false;
      }.bind(_this)
    };
  };

  if (opts.initialSets > 0) {

    var log = (opts.consistency > 0 || opts.delConsistency) ? true : opts.log;

    console.log('OK WE HAVE ' + opts.initialSets + ' INITIAL SETS TO DO:::');

    _this.startSetActivitySequential({testId: opts.testId, limit: opts.initialSets, log:log, setOptions:{upsertType:2}}, function(e, totals, logs){

      if (e) {
        _this.__emit('initial-sets-failed', {error:e.toString()});
        return;
      }

      if (opts.consistency) {

        consistencyItems = _this.__getRandomLogItems(logs, opts.consistency);
      }

      if (opts.delConsistency) {

        delConsistencyItems = _this.__getRandomLogItems(logs, opts.delConsistency, consistencyItems);

        //console.log('delConsistencyAmount:::', opts.delConsistency);
        //console.log('delConsistencyItems:::', delConsistencyItems.length);

        //this happens elsewhere, when we are verifying
        // return _this.__deleteAll(delConsistencyItems, function(e){
        //
        //   if (e) _this.__emit('error', new Error('failed deleting delConsistency list'));
        //
        //   _this.__emit('initial-sets-complete', doGetActivity());
        // });
      }

      _this.__emit('initial-sets-complete', doGetActivity());

    });
  } else return doGetActivity();
};

module.exports = RandomClient;