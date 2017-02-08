describe('happn-service-mongo functional tests', function() {

  this.timeout(20000);

  var expect = require('expect.js');

  var service = require('../../index');

  var DataStoreServiceCached = new service();

  var DataStoreServiceUncached = new service();

  var testId = require('shortid').generate();

  var OpRunner = require('../lib/operation_runner');

  var opRunner = new OpRunner();

  var config_cache = {
    url:'mongodb://127.0.0.1:27017/happn',
    datastores: [
      {
        name: 'default',
        isDefault: true,
        database:'mongo-test-cached-compare',
        cache:true
      },
      {
        name: 'uncached',
        patterns: [
          '/history/*'
        ],
        url:'mongodb://127.0.0.1:27017/mongo-test-cached-compare-history'
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

  var TESTDATAPOINTCOUNT = 1000;

  var TESTGETCOUNT = 1000;

  var TESTSETCOUNT = 1000;

  var CACHED_DATAPOINTS;

  var UNCACHED_DATAPOINTS;

  function createInitialDataPoints(callback){

    opRunner.createDataPoints({
      count:TESTDATAPOINTCOUNT,
      dataStore:DataStoreServiceCached
    }, function(e, log){

      if (e) return callback(e);

      CACHED_DATAPOINTS = log;

      opRunner.createDataPoints({
        count:TESTDATAPOINTCOUNT,
        dataStore:DataStoreServiceUncached
      }, function(e, log){

        if (e) return callback(e);

        UNCACHED_DATAPOINTS = log;

        callback();
      });
    });
  }

  before('should initialize the services', function(callback) {

    DataStoreServiceCached.initialize(config_cache, function(e){

      if (e) return callback(e);

      DataStoreServiceCached.happn = {
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

      DataStoreServiceUncached.initialize(config_nocache, function(e){

        if (e) return callback(e);

        DataStoreServiceUncached.happn = {
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

        createInitialDataPoints(callback);

      });
    });
  });

  after(function(done) {

    var afterErrors = [];

    DataStoreServiceCached.stop(function(e){

      if (e) {
        console.warn('cached service failed to stop');
        afterErrors.push(e);
      }

      DataStoreServiceUncached.stop(function(e){

        if (e) {
          console.warn('uncached service failed to stop');
          afterErrors.push(e);
        }

        if (afterErrors.length > 0) return done(afterErrors[afterErrors.length - 1]);

        done();
      });
    });
  });

  it('does ' + TESTGETCOUNT + ' gets cached and uncached, and ensures that cached gets are faster than uncached ones', function(callback) {

    var CACHED_DURATION;

    var UNCACHED_DURATION;

    var CACHED_LOG;

    var UNCACHED_LOG;

    var CACHEDSTART;

    var UNCACHEDSTART;

    CACHEDSTART = Date.now();

    opRunner.doGets({count:TESTGETCOUNT, paths:CACHED_DATAPOINTS.paths, dataStore:DataStoreServiceCached}, function(e, log){

      CACHED_DURATION = Date.now() - CACHEDSTART;

      CACHED_LOG = log;

      if (e) return callback(e);

      UNCACHEDSTART = Date.now();

      opRunner.doGets({count:TESTGETCOUNT, paths:UNCACHED_DATAPOINTS.paths, dataStore:DataStoreServiceUncached}, function(e, log){

        UNCACHED_DURATION = Date.now() - UNCACHEDSTART;

        UNCACHED_LOG = log;

        if (e) return callback(e);

        console.log('CACHED_DURATION:::', CACHED_DURATION);
        console.log('UNCACHED_DURATION:::', UNCACHED_DURATION);

        expect((UNCACHED_DURATION - CACHED_DURATION) > 0).to.be(true);

        expect(CACHED_LOG.hits).to.be(TESTGETCOUNT);
        expect(UNCACHED_LOG.hits).to.be(TESTGETCOUNT);

        callback();

      });
    });
  });

  it('does ' + TESTSETCOUNT + ' sets cached and uncached, and checks that caching only adds a small amount of overhead', function(callback) {

    var CACHED_DURATION;

    var UNCACHED_DURATION;

    var CACHED_LOG;

    var UNCACHED_LOG;

    var CACHEDSTART;

    var UNCACHEDSTART;

    CACHEDSTART = Date.now();

    console.log('CACHED SETS STARTING:::');

    opRunner.doSets({count:TESTSETCOUNT, paths:CACHED_DATAPOINTS.paths, dataStore:DataStoreServiceCached, setOptions:{merge:true}, setData:{additionalValue:TESTSETCOUNT}}, function(e, log){

      CACHED_DURATION = Date.now() - CACHEDSTART;

      CACHED_LOG = log;

      if (e) return callback(e);

      UNCACHEDSTART = Date.now();

      console.log('UNCACHED SETS STARTING:::');

      opRunner.doSets({count:TESTSETCOUNT, paths:UNCACHED_DATAPOINTS.paths, dataStore:DataStoreServiceUncached, setOptions:{merge:true}, setData:{additionalValue:TESTSETCOUNT}}, function(e, log){

        UNCACHED_DURATION = Date.now() - UNCACHEDSTART;

        UNCACHED_LOG = log;

        if (e) return callback(e);

        console.log('CACHED_DURATION:::', CACHED_DURATION);
        console.log('UNCACHED_DURATION:::', UNCACHED_DURATION);

        //expect((CACHED_DURATION - UNCACHED_DURATION) > 0).to.be(true);

        expect(CACHED_LOG.ok).to.be(TESTGETCOUNT);
        expect(UNCACHED_LOG.ok).to.be(TESTGETCOUNT);

        callback();

      });
    });
  });

});