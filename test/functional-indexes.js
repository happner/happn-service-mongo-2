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
    services:{
      data:{
        config:{
          datastores:[
            {
              name:'mongo',
              provider:db_path,
              database:'indexes_default_test'
            }
          ]
        }
      }
    }
  };

  var indexesConfig = {
    services:{
      data:{
        config:{
          datastores:[
            {
              name:'mongo',
              provider:db_path,
              database:'indexes_configured_test',
              index:{
                "happn_path_index":{
                  fields:{path: 1},
                  options:{unique: true, w: 1}
                },
                "another_index":{
                  fields:{test: 1},
                  options:{w: 1}
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

    defaultclient.get('/_SYSTEM/INDEXES/*', null, function (e, results) {

      if (e) return done(e);

      expect(results.length == 2).to.be(true);

      results.forEach(function(indexRecord){

        if (indexRecord._meta.path == 'happn_path_index'){

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
});
