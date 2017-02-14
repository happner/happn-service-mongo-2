var EventEmitter = require("events").EventEmitter
  , async = require('async')
  , path = require('path')
  ;

function TestCluster(){

  this.__events = new EventEmitter();
  this.__clients = {};
  this.__logs = {};
  this.__completed = [];
  this.__started = [];

  this.__totals = {};

  this.__setTotals = [];
  this.__getTotals = [];

  this.__consistencyTotals = {};

  this.__delConsistencyTotals = {};
}

TestCluster.prototype.__emit = function(event, message){
  return this.__events.emit(event, message);
};

TestCluster.prototype.on = function(event, handler){
  return this.__events.on(event, handler);
};

TestCluster.prototype.off = TestCluster.prototype.removeListener = function(event, handler){
  this.__events.removeListener(event, handler);
};

TestCluster.prototype.addClient = function(args, callback){

  this.__clients[args[1]] = {args:args};

  this.__logs[args[1]] = [];

  var client = this.__clients[args[1]];

  var fork = require('child_process').fork;

  client.remote = fork(path.resolve(__dirname, 'random_client_runner'), client.args);

  var _this = this;

  client.remote.on('message', function(serialized) {

    var message = JSON.parse(serialized);

    _this.__handleRemoteMessage(message);

    if (message.originId == args[1] && message.event == 'UP') {
      console.log('random client runner up::: ' + message.originId);
      return callback();
    }

    if (message.originId == args[1] && message.event == 'NOT-UP') return callback(new Error('failed to bring test client runner up'));

  });

  var config = {
    url:'mongodb://127.0.0.1:27017/happn',
    datastores: [
      {
        name: 'default',
        isDefault: true,
        database:'mongo-test-cached-stress-multiple',
        cache:true
      },
      {
        name: 'uncached',
        patterns: [
          '/history/*'
        ],
        url:'mongodb://127.0.0.1:27017/mongo-test-cached-stress-multiple-history'
      }
    ]
  };

  client.remote.send(JSON.stringify({type:'doInit', config:config}));
};

TestCluster.prototype.__aggregateTotals = function(){

};

TestCluster.prototype.verifyConsistency = function(callback){

  var _this = this;

  var idDown = Object.keys(_this.__consistencyTotals);

  var idUp = Object.keys(_this.__consistencyTotals).reverse();

  var misses = 0;

  async.eachSeries(idDown, function(idDownItem, idDownItemCB){

    async.eachSeries(idUp, function(idUpItem, idUpItemCB){

      if (idDownItem != idUpItem){

        console.log('checking consistency checking ' + idUpItem + '\'s data on ' + idDownItem);

        var deduplicateMessages = {};

        async.eachSeries(_this.__consistencyTotals[idUpItem], function(logItem, logItemCB){

          var timedOut = false;

          var doGetTimeout = setTimeout(function(){
            console.log('ttl miss:::');
            misses++;
            timedOut = true;
            logItemCB();
          }, 1000);

          _this.__clients[idDownItem].remote.on('message', function(serialized){

            var deserialized = JSON.parse(serialized);

            if (deserialized.event == 'data-get-complete' &&
              deserialized.message.message.path == logItem.path){

              if (deduplicateMessages[deserialized.message.message.path]){

                clearTimeout(doGetTimeout);
              } else {

                deduplicateMessages[deserialized.message.message.path] = true;

                clearTimeout(doGetTimeout);

                if (deserialized.message.data.data == null || deserialized.message.data.data.test != logItem.value.test) misses++;

                logItemCB();
              }
            }
          });

          _this.__clients[idDownItem].remote.send(JSON.stringify({type:'doGet', path:logItem.path}));

        }, idUpItemCB);

      } else idUpItemCB();

    }, idDownItemCB);

  }, function(e){

    if (e) return callback(e);

    callback(null, misses);

  });
};

TestCluster.prototype.verifyDelConsistencyItemsDontExist = function(clientKey, items, callback){

  var _this = this;

  var client = _this.__clients[clientKey];

  var found = 0;

  var failed = 0;

  var verified = {};

  var finishVerify = function(){

    items.forEach(function(item){

      if (!verified[item.path]) failed++;

      if (verified[item.path].data != null) found++;

    });

    if (found > 0){
      console.log('DAMN FOUND THE FOLLOWING ON ' + clientKey + ':::', foundItems);
    }

    callback(null, found, failed);
  };

  var handleRemoteMessage = function(serialized){

    var deserialized = JSON.parse(serialized);

    console.log('delete verify message::::', deserialized);

    if (deserialized.event === 'data-get-complete') {

      verified[deserialized.message.message.path] = {data:deserialized.message.message.data};
    }
  };

  client.remote.on('message', handleRemoteMessage);

  async.each(items, function(item, itemCB){

    console.log('CHECKING ITEM:::' + item.path);
    console.log('CLIENT KEY:::' + clientKey);

    client.remote.send(JSON.stringify({type:'doGet', path:item.path}));

    setTimeout(itemCB, 300);// wait a 3rd of a sec

  }, function(e){

    console.log('WAITING FOR GETS TO COMPLETE...');

    setTimeout(finishVerify, 5000);
  });

};

TestCluster.prototype.verifyDelConsistency = function(callback){

  var _this = this;

  var idDown = Object.keys(_this.__delConsistencyTotals);

  var idUp = Object.keys(_this.__delConsistencyTotals).reverse();

  var pairs = [];

  idDown.forEach(function(idDownItem, i){
    pairs.push([idDownItem, idUp[i]]);
  });

  var found = 0;

  var failed = 0;

  async.eachSeries(pairs, function(pair, pairCB){

    var idDownItem = pair[0];

    var idUpItem = pair[1];

    var toDeleteItems = _this.__delConsistencyTotals[idUpItem];

    var timedOut = false;

    var doDelTimeout = setTimeout(function(){
      //console.log('ttl del miss:::');
      misses++;
      timedOut = true;
      pairCB();
    }, 300000);

    _this.__clients[idDownItem].remote.on('message', function(serialized){

      var deserialized = JSON.parse(serialized);

      if (deserialized.event == 'delete-all-failed' &&
        deserialized.message.message.key === idUpItem){

        clearTimeout(doDelTimeout);

        return pairCB(new Error('delete all failed: ' + deserialized.message.error));
      }

      if (deserialized.event == 'delete-all-complete' &&
        deserialized.message.message.key === idUpItem){

        clearTimeout(doDelTimeout);

        setTimeout(function(){

          _this.verifyDelConsistencyItemsDontExist(idUpItem, toDeleteItems, function(e, vfound, vfailed){

            if (e) return pairCB(e);

            found += vfound;

            failed += vfailed;

            pairCB();
          });

        }, 5000);
      }
    });

    _this.__clients[idDownItem].remote.send(JSON.stringify({type:'doDeleteAll', key:idUpItem, items:toDeleteItems}));

  }, function(e){

    if (e) return callback(e);
    ////console.log('CALLING BACK WITH MISSES:::', misses);
    callback(null, found, failed);
  });
};

TestCluster.prototype.__handleRemoteMessage = function(message){

  var _this = this;

  _this.__logs[message.originId].push(message);

  _this.__emit(message.event, message);

  if (message.event == "set-activity-run-complete"){

    _this.__started.push(message.originId);

    _this.__setTotals.push(message.message.totals);
  }

  if (message.event == "get-activity-run-complete"){

    //console.log('did get run', message.originId);

    _this.__completed.push(message.originId);

    _this.__getTotals.push(message.message.totals);

    if (message.message.consistency)
      _this.__consistencyTotals[message.originId] = message.message.consistency;

    if (message.message.delConsistency)
      _this.__delConsistencyTotals[message.originId] = message.message.delConsistency;

    if (_this.__completed.length == Object.keys(_this.__clients).length)
      _this.__emit('cluster-run-complete', _this.__aggregateTotals());
  }

};

TestCluster.prototype.start = function(callback){

  var _this = this;

  async.each(Object.keys(this.__clients), function(clientId, clientCB){

    try{

      var client = _this.__clients[clientId];

      client.remote.send(JSON.stringify({type:'doRun'}));

      clientCB();

    }catch(e){

      clientCB(e);
    }

  }, function(e){

    if (e) return callback(e);

    callback();
  });
};

TestCluster.prototype.end = function(){

  var _this = this;

  Object.keys(_this.__clients).forEach(function(clientId){

    var client = _this.__clients[clientId];

    try{

      client.remote.kill();
      //console.log('killed remote:::', clientId);
    }catch(e){
      console.warn('failed killing remote client: ' + clientId);
    }
  });
};

function TestHelper(){

}

TestHelper.prototype.getCluster = function(testId, size, init, clientSize, callback, consistency, delConsistency){

  var testCluster = new TestCluster();

  async.times(size, function(time, timeCB){

    var id = testId + '_' + time;

    var arguments = [];

    arguments.push('--id');

    arguments.push(id);

    arguments.push('--cache_id');

    arguments.push(testId);

    arguments.push('--mode');

    arguments.push('fixed_throughput');

    arguments.push('--init');

    arguments.push(init);

    arguments.push('--size');

    arguments.push(clientSize);

    arguments.push('--defer');

    if (consistency){
      // will do consistency amount of random sets and add them to the final log

      arguments.push('--consistency');
      arguments.push(consistency);
    }

    if (delConsistency){
      // will do consistency amount of random sets and add them to the final log

      arguments.push('--delConsistency');
      arguments.push(delConsistency);
    }

    testCluster.addClient(arguments, timeCB);

  }, function(e){

    if (e) return callback(e);

    callback(null, testCluster);

  });
};

module.exports = TestHelper;