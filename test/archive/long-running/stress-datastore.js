describe('stress-series', function() {

  this.timeout(20000);

  var expect = require('expect.js');

  var service = require('../../index');

  var serviceInstance = new service();

  var async = require('async');

  var config = {

    url:'mongodb://127.0.0.1:27017',

    database:'happn-mongo-stress-datastore',

    collection:'happn-mongo-stress-datastore-coll',

    datastores: [
      {
        name: 'default',
        isDefault: true
      },
      {
        name: 'historical-batched',
        patterns: [
          '/datastore-batched/*'
        ],
        collection:'happn-mongo-stress-datastore-history-batched',
        policy:{
          set:{
            batchSize:70
          }
        }
      },
      {
        name: 'historical-batched-no-index',
        patterns: [
          '/datastore-batched-no-index/*'
        ],
        collection:'happn-mongo-stress-datastore-history-batched-no-index',
        policy:{
          set:{
            batchSize:70
          }
        },
        index:false
      },
      {
        name: 'historical-upsert',
        patterns: [
          '/datastore-upsert/*'
        ],
        collection:'happn-mongo-stress-datastore-history-upsert'
      },
      {
        name: 'historical-insert',
        patterns: [
          '/datastore-insert/*'
        ],
        collection:'happn-mongo-stress-datastore-history-insert'
      },
      {
        name: 'historical-update',
        patterns: [
          '/datastore-update/*'
        ],
        collection:'happn-mongo-stress-datastore-history-update'
      }
    ]
  };

  var clearCollections = function (callback) {

    serviceInstance.remove('/datastore-batched/*', {}, function (e) {
      if (e) return callback(e);
      serviceInstance.remove('/datastore-batched-no-index/*', {}, function (e) {
        if (e) return callback(e);
        serviceInstance.remove('/datastore-upsert/*', {}, function (e) {
          if (e) return callback(e);
          serviceInstance.remove('/datastore-insert/*', {}, function (e) {
            if (e) return callback(e);
            serviceInstance.remove('/datastore-update/*', {}, callback);
          });
        });
      });
    });
  };

  before('should initialize the service', function(callback) {

    serviceInstance.initialize(config, function(e){

      if (e) return callback(e);

      serviceInstance.happn = {
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

      clearCollections(function(e){
        console.log('cleared collections:::');
        callback();
      });

    });
  });

  after(function(done) {

    serviceInstance.stop(done);
  });

  var DURATION = 10000; //10 seconds

  // context('system', function(){
  //
  //   it.only('clears the collections', function(callback) {
  //     clearCollections(callback);
  //   });
  //
  // });

  it.only('tests set throughput , batch insert, no index', function(callback) {

    this.timeout(DURATION + 10000);

    var TESTING = true;
    var SETCOUNTER = 0;
    var ERRCOUNTER = 0;
    var TRIEDCOUNTER = 0;

    setTimeout(function(){
      TESTING = false;
    }, DURATION);

    var doSet = function(){

      TRIEDCOUNTER ++;

      serviceInstance.upsert('/datastore-batched-no-index/' + TRIEDCOUNTER.toString(), {"test":SETCOUNTER}, {upsertType:2}, function(e){

        SETCOUNTER++;

        if (e) {
          console.log('error:::', e);
           ERRCOUNTER++;
        }

      });

      if (!TESTING) {
        console.log('did ' + SETCOUNTER + ' insert sets out of ' + TRIEDCOUNTER + ' attempts in ' + DURATION + ' milliseconds');
        callback();
      } else setImmediate(doSet);
    };

    doSet();

  });

  it('tests set throughput , batch insert', function(callback) {

    this.timeout(DURATION + 10000);

    var TESTING = true;
    var SETCOUNTER = 0;
    var ERRCOUNTER = 0;
    var TRIEDCOUNTER = 0;

    setTimeout(function(){
      TESTING = false;
    }, DURATION);

    var doSet = function(){

      TRIEDCOUNTER ++;

      serviceInstance.upsert('/datastore-batched/' + TRIEDCOUNTER.toString(), {"test":SETCOUNTER}, {upsertType:2}, function(e){

        SETCOUNTER++;

        if (e) {
          console.log('error:::', e);
          TESTING = false;
        }

      });

      if (!TESTING) {
        console.log('did ' + SETCOUNTER + ' insert sets out of ' + TRIEDCOUNTER + ' attempts in ' + DURATION + ' milliseconds');
        callback();
      } else setImmediate(doSet);
    };

    doSet();

  });

  it('tests set throughput, update', function(callback) {

    this.timeout(DURATION + 1000);

    var TESTING = true;
    var SETCOUNTER = 0;

    setTimeout(function(){
      TESTING = false;
    }, DURATION);

    async.whilst(function(){
      return TESTING;
    }, function(whilstCB){

      serviceInstance.upsert('/history_update/' + SETCOUNTER.toString(), {"test":SETCOUNTER}, {upsertType:1}, function(e){

        if (e) return whilstCB(e);

        SETCOUNTER++;

        whilstCB();
      });

    }, function(e){

      if (e) return callback(e);

      console.log('did ' + SETCOUNTER.toString() + ' update sets in ' + (DURATION / 1000).toString() + ' seconds');

      callback();
    });
  });

  it('tests set throughput, insert', function(callback) {

    this.timeout(DURATION + 1000);

    var TESTING = true;
    var SETCOUNTER = 0;

    setTimeout(function(){
      TESTING = false;
    }, DURATION);

    async.whilst(function(){
      return TESTING;
    }, function(whilstCB){

      serviceInstance.upsert('/history_insert/' + SETCOUNTER.toString(), {"test":SETCOUNTER}, {upsertType:2}, function(e){

        if (e) return whilstCB(e);

        SETCOUNTER++;

        whilstCB();
      });

    }, function(e){

      if (e) return callback(e);

      console.log('did ' + SETCOUNTER.toString() + ' insert sets in ' + (DURATION / 1000).toString() + ' seconds');

      callback();
    });
  });

  it('tests get throughput, exact path', function(callback) {

    this.timeout((DURATION / 100) * 500 + (DURATION + 2000));

    var TESTING = true;
    var GETCOUNTER = 0;

    async.times(DURATION / 100, function(time, timeCB){

      serviceInstance.upsert('/app_land/' + time.toString(), {"test":time}, {}, timeCB);
    }, function(e){

      if (e) return callback(e);

      setTimeout(function(){
        TESTING = false;
      }, DURATION);

      async.whilst(function(){
        return TESTING;
      }, function(whilstCB){

        serviceInstance.get('/app_land/' + GETCOUNTER.toString(), {}, function(e){

          if (e) return whilstCB(e);

          GETCOUNTER++;

          whilstCB();
        });

      }, function(e){

        if (e) return callback(e);

        console.log('did ' + GETCOUNTER.toString() + ' gets in ' + (DURATION / 1000).toString() + ' seconds');

        callback();
      });

    });
  });

  it('tests get throughput, wildcard path', function(callback) {

    this.timeout((DURATION / 100) * 500 + (DURATION + 2000));

    var TESTING = true;
    var GETCOUNTER = 0;

    console.log('upserting');

    var ids = [];

    async.times(DURATION, function(time, timeCB){

      var id = require('shortid').generate();

      ids.push(id);

      serviceInstance.upsert('/app_land_wild/' + id + '/' + time.toString(), {"test":time}, {}, timeCB);

    }, function(e){

      if (e) return callback(e);

      console.log('upserted');

      setTimeout(function(){
        TESTING = false;
      }, DURATION);

      async.whilst(function(){
        return TESTING;
      }, function(whilstCB){

        serviceInstance.get('/app_land_wild/*/' + GETCOUNTER.toString(), {}, function(e){

          if (e) return whilstCB(e);

          GETCOUNTER++;

          whilstCB();
        });

      }, function(e){

        if (e) return callback(e);

        console.log('did ' + GETCOUNTER.toString() + ' wildcard gets in ' + (DURATION / 1000).toString() + ' seconds');

        callback();
      });

    });
  });

  it('tests remove throughput', function(callback) {

    this.timeout((DURATION / 100) * 500 + (DURATION + 2000));

    var TESTING = true;
    var REMOVECOUNTER = 0;

    async.times(DURATION, function(time, timeCB){

      serviceInstance.upsert('/scratch/' + time.toString(), {"test":time}, {}, timeCB);
    }, function(e){

      if (e) return callback(e);

      setTimeout(function(){
        TESTING = false;
      }, DURATION);

      async.whilst(function(){
        return TESTING;
      }, function(whilstCB){

        serviceInstance.remove('/scratch/' + REMOVECOUNTER.toString(), {}, function(e){

          if (e) return whilstCB(e);

          REMOVECOUNTER ++;

          whilstCB();

        });

      }, function(e){

        if (e) return callback(e);

        console.log('did ' + REMOVECOUNTER.toString() + ' removes in ' + (DURATION / 1000).toString() + ' seconds');

        callback();
      });

    });
  });

  it('tests remove throughput, wildcard', function(callback) {

    this.timeout((DURATION / 100) * 500 + (DURATION + 2000));

    var TESTING = true;
    var REMOVECOUNTER = 0;

    console.log('upserting');

    async.times(DURATION, function(time, timeCB){

      serviceInstance.upsert('/scratch_wild/' + time.toString(), {"test":time}, {}, timeCB);
    }, function(e){

      if (e) return callback(e);

      console.log('upserted');

      setTimeout(function(){
        TESTING = false;
      }, DURATION);

      async.whilst(function(){
        return TESTING;
      }, function(whilstCB){

        serviceInstance.remove('/scratch_wild/' + REMOVECOUNTER.toString(), {}, function(e){

          if (e) return whilstCB(e);

          REMOVECOUNTER ++;

          whilstCB();

        });

      }, function(e){

        if (e) return callback(e);

        console.log('did ' + REMOVECOUNTER.toString() + ' wildcard removes in ' + (DURATION / 1000).toString() + ' seconds');

        callback();
      });

    });
  });
});