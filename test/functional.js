describe('happn-service-mongo functional tests', function() {

  this.timeout(20000);

  var expect = require('expect.js');

  var service = require('../service');
  var serviceInstance = new service();

  var testId = require('shortid').generate();

  var config = {
    url:'mongodb://127.0.0.1:27017/happn'
  }

  before('should initialize the service', function(callback) {

    serviceInstance.initialize(config, callback);

  });

  after(function(done) {

    serviceInstance.stop(done);

  });

  it('sets data', function(callback) {

    serviceInstance.upsert('/set/' + testId, {"test":"data"}, {}, function(e, response){

      if (e) return callback(e);

      console.log('set response:::', response);

      expect(response.data.test).to.equal("data");

      callback();

    });

  });

  it('merges data', function(callback) {

    serviceInstance.upsert('/merge/' + testId, {"test":"data"}, {}, function(e, response){

      if (e) return callback(e);

      serviceInstance.upsert('/merge/' + testId, {"test1":"data1"}, {merge:true}, function(e, response){

        if (e) return callback(e);

        console.log('set response:::', response);

        callback();

      });


    });

  });

  xit('tags data', function(callback) {

  });

  xit('removes data', function(callback) {

  });

  xit('gets data', function(callback) {

  });

  xit('gets data with wildcard', function(callback) {

  });

  xit('gets data with complex search', function(callback) {

  });

});