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

  it('tags data', function(callback) {

    var tag = require("shortid").generate();

    serviceInstance.upsert('/tag/' + testId, {"test":"data"}, {}, function(e, response){

      if (e) return callback(e);

      serviceInstance.upsert('/tag/' + testId, {"test":"data"}, {"tag":tag}, function(e, response){

        if (e) return callback(e);

        expect(response.data.path).to.equal('/tag/' + testId);
        expect(response.data.data.test).to.equal('data');
        expect(response._meta.tag).to.equal(tag);
        expect(response._meta.path.indexOf('/_TAGS' + '/tag/' + testId)).to.equal(0);

        callback();

      });

    });

  });

  it('removes data', function(callback) {

     serviceInstance.upsert('/remove/' + testId, {"test":"data"}, {}, function(e, response){

      if (e) return callback(e);

      serviceInstance.remove('/remove/' + testId, {}, function(e, response){

        if (e) return callback(e);

        expect(response._meta.path).to.equal('/remove/' + testId);
        expect(response.data.removed.result.n).to.equal(1);

        callback();

      });

    });

  });

   it('removes multiple data', function(callback) {

     serviceInstance.upsert('/remove/multiple/1/' + testId, {"test":"data"}, {}, function(e, response){

      if (e) return callback(e);

      serviceInstance.upsert('/remove/multiple/2/' + testId, {"test":"data"}, {}, function(e, response){

        if (e) return callback(e);

        serviceInstance.remove('/remove/multiple/*/' + testId, {}, function(e, response){

          if (e) return callback(e);

          expect(response._meta.path).to.equal('/remove/multiple/*/' + testId);
          expect(response.data.removed.result.n).to.equal(2);

          callback();

        });

      });

    });

  });

  it('gets data with wildcard', function(callback) {

    serviceInstance.upsert('/get/multiple/1/' + testId, {"test":"data"}, {}, function(e, response){

      if (e) return callback(e);

      serviceInstance.upsert('/get/multiple/2/' + testId, {"test":"data"}, {}, function(e, response){

        if (e) return callback(e);

         serviceInstance.get('/get/multiple/*/' + testId, {}, function(e, response){

            //console.log('get multiple response:::', response);

            expect(response.length).to.equal(2);
            expect(response[0].data.test).to.equal('data');
            expect(response[0]._meta.path).to.equal('/get/multiple/1/' + testId);
            expect(response[1].data.test).to.equal('data');
            expect(response[1]._meta.path).to.equal('/get/multiple/2/' + testId);

            callback();

         });

      });

    });


  });

  xit('gets data with complex search', function(callback) {

  });

});