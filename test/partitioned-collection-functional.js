describe('happn-service-mongo functional tests', function() {

  this.timeout(20000);

  var expect = require('expect.js');

  var service = require('../index');

  var serviceInstance = new service();

  var async = require('async');

  var config = {

    url:'mongodb://127.0.0.1:27017',

    database:'partitioned-collection-functional-db',

    collection:'partitioned-collection-functional-coll',

    datastores: [
      {
        name: 'default',
        isDefault: true,
        database:'mongo-partitioned-test-default'
      },
      {
        name: 'test4',
        patterns: [
          '/test4/test'
        ],
        url:'mongodb://127.0.0.1:27017/mongo-partitioned-test-test4'
      },
      {
        name: 'test2_3',
        patterns: [
          '/test2/*',
          '/test3/*'
        ],
        collection:'mongo-partitioned-test-test2_3'
      },
      {
        name: 'test1',
        patterns: [
          '/test1/*'
        ],
        collection:'mongo-partitioned-test-test1'
      }
    ]
  };

  var clearCollections = function(callback){

    serviceInstance.remove('/test1/*', {}, function(e, response){

      if (e) return callback(e);

      serviceInstance.remove('/test2/*', {}, function(e, response){

        if (e) return callback(e);

        serviceInstance.remove('/test3/*', {}, function(e, response){

          if (e) return callback(e);

          serviceInstance.remove('/test4/*', {}, function(e, response){

            if (e) return callback(e);

            serviceInstance.remove('/*', {}, function(e, response){

              if (e) return callback(e);

              callback();

            });

          });
        });
      });
    });
  };

  before('should initialize the service', function(callback) {

    serviceInstance.initialize(config, function(e){

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

      clearCollections(callback);

    });
  });

  after(function(done) {

    serviceInstance.stop(done);
  });

  var Datastore = require('mongodb');
  var MongoClient = Datastore.MongoClient;

  it('test1', function(callback) {

    async.eachSeries([0,1,2,3,4], function(counter, eachCB){

      serviceInstance.upsert('/test1/' + counter.toString(), {"test":counter}, {}, eachCB);

    }, function(e){

      if (e) return callback(e);

      MongoClient.connect(config.url + '/mongo-partitioned-test-test1', {}, function (e, db) {

        if (e) return callback(e);

        db.collection('mongo-partitioned-test-test1')

          .find()

          .toArray(function(e, items){

          if (e) return callback(e);

          expect(items.length).to.be(5);

          callback();
        });
      });
    });
  });

  it('test2_3', function(callback) {

    async.eachSeries([0,1,2,3,4], function(counter, eachCB){

      serviceInstance.upsert('/test2/' + counter.toString(), {"test":counter}, {}, function(e){

        if (e) return callback(e);

        serviceInstance.upsert('/test3/' + counter.toString(), {"test":counter}, {}, function(e){

          if (e) return callback(e);

          eachCB();

        });
      });

    }, function(e){

      if (e) return callback(e);

      MongoClient.connect(config.url + '/mongo-partitioned-test-test2_3', {}, function (e, db) {

        if (e) return callback(e);

        db.collection('mongo-partitioned-test-test2_3')

          .find()

          .toArray(function(e, items){

            if (e) return callback(e);

            expect(items.length).to.be(10);

            callback();
          });
      });
    });
  });

  it('test4', function(callback) {

    async.eachSeries([0,1,2,3,4], function(counter, eachCB){

      serviceInstance.upsert('/test4/test', {"test":counter}, {}, function(e){

        if (e) return callback(e);

        serviceInstance.upsert('/blahblah/' + counter.toString(), {"test":counter}, {}, function(e){

          if (e) return callback(e);

          eachCB();

        });
      });

    }, function(e){

      if (e) return callback(e);

      MongoClient.connect(config.url + '/mongo-partitioned-test-test4', {}, function (e, db) {

        if (e) return callback(e);

        db.collection('mongo-partitioned-test-test4')

          .find()

          .toArray(function(e, items){

            if (e) return callback(e);

            expect(items.length).to.be(1);

            MongoClient.connect(config.url + '/mongo-partitioned-test-default', {}, function (e, db) {

              db.collection('mongo-partitioned-test-default')

                .find()

                .toArray(function(e, items){

                  if (e) return callback(e);

                  expect(items.length).to.be(5);

                  callback();
                });
            });
          });
      });
    });
  });
});