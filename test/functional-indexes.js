describe('indexes-tests', function () {

  var expect = require('expect.js');
  var happn = require('happn-3');
  var service = happn.service;
  var async = require('async');
  var test_secret = 'test_secret';

  var defaultHappnInstance = null;

  var indexedHappnInstance = null;

  var test_id;
  var path = require('path');

  this.timeout(5000);

  var db_path = path.resolve(__dirname.replace('test',''))  + path.sep + 'index.js';

  var defaultConfig = {
    port:55001,
    services:{
      data:{
        config:{
          datastores:[
            {
              name:'mongo',
              provider:db_path,
              settings: {
                database: 'indexes_default_test_db',
                collection: 'indexes_default_test_db_coll'
              }
            }
          ]
        }
      }
    }
  };

  var indexesConfig = {
    port:55002,
    services:{
      data:{
        config:{
          datastores:[
            {
              name:'mongo',
              provider:db_path,
              settings: {
                database: 'indexes_configured_test_db',
                collection:'indexes_configured_test_db_coll',
                index: {
                  "happn_path_index": {
                    fields: {path: 1},
                    options: {unique: true, w: 1}
                  },
                  "another_index": {
                    fields: {test: 1},
                    options: {w: 1}
                  }
                }
              }
            }
          ]
        }
      }
    }
  };

  before('should initialize the default service', function (callback) {

    test_id = Date.now() + '_' + require('shortid').generate();

    try {

      service.create(defaultConfig,

        function (e, happnInst) {

          if (e) return callback(e);

          defaultHappnInstance = happnInst;

          callback();
        });
    } catch (e) {
      callback(e);
    }
  });

  before('should initialize the indexed service', function (callback) {

    test_id = Date.now() + '_' + require('shortid').generate();

    try {

      service.create(indexesConfig,

        function (e, happnInst) {

          if (e) return callback(e);

          indexedHappnInstance = happnInst;

          callback();
        });
    } catch (e) {
      callback(e);
    }
  });

  after(function (done) {
    if (defaultHappnInstance) defaultHappnInstance.stop(done);
    else done();
  });

  after(function (done) {
    if (indexedHappnInstance) indexedHappnInstance.stop(done);
    else done();
  });

  var defaultclient;
  var indexedclient;

  /*
   We are initializing 2 clients to test saving data against the database, one client will push data into the
   database whilst another listens for changes.
   */
  before('should initialize the clients', function (callback) {

    try {

      defaultHappnInstance.services.session.localClient(function(e, instance){

        if (e) return callback(e);
        defaultclient = instance;

        indexedHappnInstance.services.session.localClient(function(e, instance){

          if (e) return callback(e);
          indexedclient = instance;

          callback();
        });
      });

    } catch (e) {
      callback(e);
    }
  });

  it('should find the default index record', function (done) {

    defaultclient.get('/_SYSTEM/INDEXES/happn_path_index', null, function (e, result) {

      if (e) return done(e);

      expect(result).to.not.be(null);
      expect(result).to.not.be(undefined);

      expect(result.fields).to.eql({path: 1});
      expect(result.options).to.eql({unique: true, w: 1});

      done();
    });
  });

  it('should find the configured index records', function (done) {

    indexedclient.get('/_SYSTEM/INDEXES/*', null, function (e, results) {

      if (e) return done(e);

      expect(results.length == 2).to.be(true);

      results.forEach(function(indexRecord){

        if (indexRecord._meta.path == '/_SYSTEM/INDEXES/happn_path_index'){

          expect(indexRecord.fields).to.eql({path: 1});
          expect(indexRecord.options).to.eql({unique: true, w: 1});

        } else {

          expect(indexRecord.fields).to.eql({test: 1});
          expect(indexRecord.options).to.eql({w: 1});
        }
      });

      done();
    });
  });

  it('should find the configured indexes in mongo', function (done) {

    if (process.env.INTRAVENOUS === 'yes') return done();

    var  mongodb = require('mongodb')
      ,  mongoclient = mongodb.MongoClient;

    mongoclient.connect ("mongodb://127.0.0.1:27017",{}, function (err, client) {

      if (err) return callback(err);

      var database = client.db('indexes_configured_test_db');

      var collection = database.collection("indexes_configured_test_db_coll");

      collection.listIndexes().toArray(function(e, indexes){

        var happn_path_index_1 = false;
        var test_1 = false;

        indexes.forEach(function(index){

          if (index.ns == "indexes_configured_test_db.indexes_configured_test_db_coll") happn_path_index_1 = true;
          if (index.name == "test_1") test_1 = true;
        });

        expect(happn_path_index_1 && test_1).to.be(true);
        done();
      });
    });
  });
});
