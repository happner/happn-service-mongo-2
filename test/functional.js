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

    var beforeCreatedOrModified = Date.now();

    setTimeout(function(){

      serviceInstance.upsert('/set/' + testId, {"test":"data"}, {}, function(e, response){

        if (e) return callback(e);

        expect(response.data.test).to.equal("data");

        expect(response._meta.created > beforeCreatedOrModified).to.equal(true);
        expect(response._meta.modified > beforeCreatedOrModified).to.equal(true);

        callback();

      });


    }, 100);

  });

  it('gets data', function(callback) {

     serviceInstance.upsert('/get/' + testId, {"test":"data"}, {}, function(e, response){

      if (e) return callback(e);

      expect(response.data.test).to.equal("data");

      serviceInstance.get('/get/' + testId, {}, function(e, response){

        if (e) return callback(e);

        expect(response._meta.path).to.equal('/get/' + testId);
        expect(response.data.test).to.equal("data");

        callback();

      });

    });

  });

  it('merges data', function(callback) {

    var initialCreated;

    serviceInstance.upsert('/merge/' + testId, {"test":"data"}, {}, function(e, response){

      if (e) return callback(e);

      initialCreated = response._meta.created;

      serviceInstance.upsert('/merge/' + testId, {"test1":"data1"}, {merge:true}, function(e, response){

        if (e) return callback(e);

        expect(response._meta.created).to.equal(initialCreated);

        serviceInstance.get('/merge/' + testId, {}, function(e, response){

          if (e) return callback(e);

          console.log('merge get response:::', response);

          expect(response.data.test).to.equal("data");
          expect(response.data.test1).to.equal("data1");
          expect(response._meta.created).to.equal(initialCreated);
          expect(response._meta.modified > initialCreated).to.equal(true);

          callback();

        });

      });


    });

  });

  it.only('tags data', function(callback) {

    var tag = require("shortid").generate();

    serviceInstance.upsert('/tag/' + testId, {"test":"data"}, {}, function(e, response){

      if (e) return callback(e);

      console.log('doing a tag:::', response);

      serviceInstance.upsert('/tag/' + testId, {"test":"data"}, {"tag":tag}, function(e, response){

        if (e) return callback(e);

        console.log('tag response:::', response);


      });

    });

  });

  xit('removes data', function(callback) {

  });

  xit('gets data with wildcard', function(callback) {

  });

  xit('gets data with complex search', function(callback) {

  });

});