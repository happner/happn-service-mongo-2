var filename = require('path').basename(__filename);

describe('integration/' + filename + '\n', function() {
  this.timeout(20000);
  var expect = require('expect.js');
  var service = require('../../index');
  var test = require('../__fixtures/happn-test-helper').create();
  var config = {
    url: 'mongodb://127.0.0.1:27017/happn'
  };

  var serviceInstance = new service(config);

  before('should clear the mongo collection', function(callback) {
    let clearMongo = require('../__fixtures/clear-mongo-collection');
    clearMongo('mongodb://localhost/happn', 'happn', callback);
  });

  before('should initialize the service', function(callback) {
    serviceInstance.initialize(function(e) {
      if (e) return callback(e);

      if (!serviceInstance.happn)
        serviceInstance.happn = {
          services: {
            utils: {
              wildcardMatch: function(pattern, matchTo) {
                var regex = new RegExp(pattern.replace(/[*]/g, '.*'));
                var matchResult = matchTo.match(regex);

                if (matchResult) return true;

                return false;
              }
            }
          }
        };

      callback();
    });
  });

  function createTestItem(id, group, custom, pathPrefix) {
    return new Promise((resolve, reject) => {
      serviceInstance.upsert(
        `${pathPrefix || ''}/searches-and-aggregation/${id}`,
        {
          data: {
            group,
            custom,
            id
          }
        },
        function(e, response, created) {
          if (e) return reject(e);
          resolve(created);
        }
      );
    });
  }

  this.timeout(60000);
  before('it creates large-ish testdata', async () => {
    for (let i = 0; i < 5000; i++) {
      await createTestItem(`largeDataset${i}`, 'largeDataset', `some test data to search on ${i}`);
    }
  });
  it('maxTimeMS causes timeout ', async () => {
    let error = null;
    let timeout = setTimeout(() => {
      error = new Error('test timed out');
    }, 120000);
    while (!error) {
      serviceInstance.find(
        '/searches-and-aggregation/*',
        {
          criteria: {
            'data.custom': { $regex: '.*data to search on 4900.*' }
          },
          options: {
            maxTimeMS: 1
          }
        },
        err => {
          error = err;
          clearTimeout(timeout);
        }
      );
      await test.delay(500);
    }
    expect(error.code).to.be(50);
  });

  after(function(done) {
    serviceInstance.stop(done);
  });
});
