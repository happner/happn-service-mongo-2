describe('stress-parallel', function() {

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

    it('tests set throughput , insert', function(callback) {

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

        serviceInstance.upsert('/history_insert_parallel/' + SETCOUNTER.toString(), {"test":SETCOUNTER}, {upsertType:2}, function(e){

          if (e) {
            ERRCOUNTER ++;
            TESTING = false;
          }
          else SETCOUNTER++;
        });

        if (!TESTING) {
          console.log('did ' + SETCOUNTER + ' insert sets out of ' + TRIEDCOUNTER + ' attempts in ' + DURATION + ' milliseconds');
          callback();
        } else setImmediate(doSet);
      };

      doSet();

    });

    it('tests set throughput , update', function(callback) {

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

        serviceInstance.upsert('/history_update_parallel/' + SETCOUNTER.toString(), {"test":SETCOUNTER}, {upsertType:1}, function(e){
          if (e) {
            ERRCOUNTER ++;
            TESTING = false;
          }
          else SETCOUNTER++;
        });

        if (!TESTING) {
          console.log('did ' + SETCOUNTER + ' update sets out of ' + TRIEDCOUNTER + ' attempts in ' + DURATION + ' milliseconds');
          callback();
        } else setImmediate(doSet);
      };

      doSet();

    });



    it('tests set throughput , upsert', function(callback) {

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

        serviceInstance.upsert('/history_parallel/' + SETCOUNTER.toString(), {"test":SETCOUNTER}, {}, function(e){
          if (e) {
            ERRCOUNTER ++;
            TESTING = false;
          }
          else SETCOUNTER++;
        });

        if (!TESTING) {
          console.log('did ' + SETCOUNTER + ' upsert sets out of ' + TRIEDCOUNTER + ' attempts in ' + DURATION + ' milliseconds');
          callback();
        } else setImmediate(doSet);
      };

      doSet();

    });

});