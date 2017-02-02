describe('stress tests - cached', function() {

  this.timeout(20000);

  var testId = require('shortid').generate();

  var INITIAL_SETS = 1000; //how many initial sets per node

  var GETS = 1000; //how many gets per node

  var config_cache = {
    url:'mongodb://127.0.0.1:27017/happn',
    datastores: [
      {
        name: 'default',
        isDefault: true,
        database:'mongo-test-cached-stress',
        cache:true
      },
      {
        name: 'uncached',
        patterns: [
          '/history/*'
        ],
        url:'mongodb://127.0.0.1:27017/mongo-test-cached-stress-history'
      }
    ]
  };

  var config_nocache = {
    url:'mongodb://127.0.0.1:27017/happn',
    datastores: [
      {
        name: 'default',
        isDefault: true,
        database:'mongo-test-uncached-compare'
      },
      {
        name: 'uncached',
        patterns: [
          '/history/*'
        ],
        url:'mongodb://127.0.0.1:27017/mongo-test-uncached-compare-history'
      }
    ]
  };

  it('checks throughput per second single process, limited sets and gets', function(callback) {

    this.timeout(600000);

    var RandomClient = require('../lib/random-client');

    var client = new RandomClient(
      {testId:testId}
    );

    client.initialize(config_cache, function(e){

      client.on('set-activity-run-complete', function(message){
        console.log('SETS-COMPLETE:::', message);
      });

      client.on('get-activity-run-complete', function(message){
        console.log('GETS-COMPLETE:::', message);
        callback();
      });

      client.startGetActivity({initialSets:INITIAL_SETS, limit:GETS, log:false});

    });
  });

  it('checks throughput per second single process no cache, limited sets and gets', function(callback) {

    this.timeout(600000);

    var RandomClient = require('../lib/random-client');

    var client = new RandomClient(
      {testId:testId}
    );

    client.initialize(config_nocache, function(e){

      client.on('set-activity-run-complete', function(message){
        console.log('SETS-COMPLETE:::', message);
      });

      client.on('get-activity-run-complete', function(message){
        console.log('GETS-COMPLETE:::', message);
        callback();
      });

      client.startGetActivity({initialSets:INITIAL_SETS, limit:GETS, log:false});

    });
  });

  var MULTIPLE_INSTANCE_COUNT = 10;

  var MULTIPLE_INSTANCE_GETS = 1000;

  it('does ' + INITIAL_SETS + ' initial sets and ' + MULTIPLE_INSTANCE_GETS + ' gets, over ' + MULTIPLE_INSTANCE_COUNT + ' instances, then it sends a verification log to each instance to ensure that the data looks the same for those instances', function(callback) {

    var _this = this;

    _this.timeout(MULTIPLE_INSTANCE_COUNT * 600000);

    var testId = require('shortid').generate();

    var TestHelper = require('../lib/helper');

    var testHelper = new TestHelper();

    testHelper.getCluster(testId, MULTIPLE_INSTANCE_COUNT, INITIAL_SETS, MULTIPLE_INSTANCE_GETS, function(e, cluster){

      if (e) return callback(e);

      cluster.on('cluster-run-complete', function(){

        cluster.end();
        callback();
      });

      cluster.start(function(e){

        if (e) return callback(e);
        console.log('STARTED:::');
      });
    });
  });

});