describe('stress-series', function() {

  this.timeout(20000);

  var expect = require('expect.js');

  var service = require('../../index');

  var serviceInstance = new service();

  var async = require('async');

  var config = {

    url:'mongodb://127.0.0.1:27017',

    database:'happn-mongo-stress-db',

    collection:'happn-mongo-stress-coll',

    datastores: [
      {
        name: 'default',
        isDefault: true,
        database:'happn-mongo-stress-default'
      },
      {
        name: 'historical',
        patterns: [
          '/history/*'
        ],
        url:'mongodb://127.0.0.1:27017',
        collection:'happn-mongo-stress-history'
      },
      {
        name: 'historical_update',
        patterns: [
          '/history_update/*'
        ],
        url:'mongodb://127.0.0.1:27017',
        collection:'happn-mongo-stress-history'
      },
      {
        name: 'historical_insert',
        patterns: [
          '/history_insert/*'
        ],
        url:'mongodb://127.0.0.1:27017',
        collection:'happn-mongo-stress-history'
      },
      {
        name: 'historical_update_parallel',
        patterns: [
          '/history_update_parallel/*'
        ],
        url:'mongodb://127.0.0.1:27017',
        collection:'happn-mongo-stress-history_parallel'
      },
      {
        name: 'historical_insert_parallel',
        patterns: [
          '/history_insert_parallel/*'
        ],
        url:'mongodb://127.0.0.1:27017',
        collection:'happn-mongo-stress-history_parallel'
      },
      {
        name: 'historical_parallel',
        patterns: [
          '/history_parallel/*'
        ],
        url:'mongodb://127.0.0.1:27017',
        collection:'happn-mongo-stress-history_parallel'
      },
      {
        name: 'app_land',
        patterns: [
          '/app_land/*'
        ],
        url:'mongodb://127.0.0.1:27017',
        collection:'happn-mongo-stress-app_land'
      },
      {
        name: 'app_land_wild',
        patterns: [
          '/app_land_wild/*'
        ],
        url:'mongodb://127.0.0.1:27017',
        collection:'happn-mongo-stress-app_land_wild'
      },
      {
        name: 'scratch',
        patterns: [
          '/scratch/*'
        ],
        url:'mongodb://127.0.0.1:27017',
        collection:'happn-mongo-stress-scratch'
      },
      {
        name: 'scratch_wild',
        patterns: [
          '/scratch_wild/*'
        ],
        url:'mongodb://127.0.0.1:27017',
        collection:'happn-mongo-stress-scratch_wild'
      }
    ]
  };

  var clearCollections = function (callback) {
    serviceInstance.remove('/history/*', {}, function (e) {
      if (e) return callback(e);
      serviceInstance.remove('/history_parallel/*', {}, function (e) {
        if (e) return callback(e);
        serviceInstance.remove('/app_land/*', {}, function (e) {
          if (e) return callback(e);
          serviceInstance.remove('/history_update/*', {}, function (e) {
            if (e) return callback(e);
            serviceInstance.remove('/history_insert/*', {}, function (e) {
              if (e) return callback(e);
              serviceInstance.remove('/history_update_parallel/*', {}, function (e) {
                if (e) return callback(e);
                serviceInstance.remove('/history_insert_parallel/*', {}, function (e) {
                  if (e) return callback(e);
                  serviceInstance.remove('/app_land_wild/*', {}, function (e) {
                    if (e) return callback(e);
                    serviceInstance.remove('/scratch/*', {}, callback);
                  });
                });
              });
            });
          });
        });
      });
    });
  };

  before('should initialize the service', function(callback) {

    serviceInstance.initialize(config, function(e){

      if (e) return callback(e);

      console.log('initialized service');

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

    it('tests set throughput , upsert', function(callback) {

      this.timeout(DURATION + 1000);

      var TESTING = true;
      var SETCOUNTER = 0;

      setTimeout(function(){
        TESTING = false;
      }, DURATION);

      async.whilst(function(){
        return TESTING;
      }, function(whilstCB){

        serviceInstance.upsert('/history/' + SETCOUNTER.toString(), {"test":SETCOUNTER}, {}, function(e){

          if (e) return whilstCB(e);

          SETCOUNTER++;

          whilstCB();
        });

      }, function(e){

        if (e) console.log('OOPS',e);

        if (e) return callback(e);

        console.log('did ' + SETCOUNTER.toString() + ' upsert sets in ' + (DURATION / 1000).toString() + ' seconds');

        callback();
      });
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