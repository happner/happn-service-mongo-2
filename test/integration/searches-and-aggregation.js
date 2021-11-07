var filename = require('path').basename(__filename);

describe('integration/' + filename + '\n', function() {
  this.timeout(20000);
  var expect = require('expect.js');
  var service = require('../../index');
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

  before('it creates test data', async () => {
    await createTestItem(1, 'odd', 'Odd');
    await createTestItem(2, 'even', 'Even');
    await createTestItem(3, 'odd', 'odd');
    await createTestItem(4, 'even', 'even');
    await createTestItem(5, 'odd', 'ODD');
    await createTestItem(6, 'even', 'EVEN');
    await createTestItem(7, 'odd', 'odD');
    await createTestItem(8, 'even', 'EVen');
    await createTestItem(9, 'odd', 'odd');
    await createTestItem(10, 'even', 'even');
    await createTestItem(11, 'even', 'even', '/other');
    await createTestItem(12, 'even', 'even', '/other');
  });

  after(function(done) {
    serviceInstance.stop(done);
  });

  it('tests a normal search', function(callback) {
    serviceInstance.find('/searches-and-aggregation/*', {}, function(e, items) {
      if (e) return callback(e);
      expect(items.length).to.be(10);
      callback();
    });
  });

  it('tests a normal search, with the count option and $not', function(callback) {
    serviceInstance.count(
      '/searches-and-aggregation/*',
      {
        criteria: {
          'data.custom': { $not: { $eq: 'Odd' } }
        }
      },
      function(e, result) {
        if (e) return callback(e);
        expect(result.data.value).to.be(9);
        callback();
      }
    );
  });

  it('tests a normal search, with the count option, collation case insensitive', function(callback) {
    serviceInstance.count(
      '/searches-and-aggregation/*',
      {
        criteria: {
          'data.custom': { $eq: 'Odd' }
        },
        options: {
          collation: {
            locale: 'en_US',
            strength: 1
          }
        }
      },
      function(e, result) {
        if (e) return callback(e);
        expect(result.data.value).to.be(5);
        callback();
      }
    );
  });

  it('tests a normal search, with the count option, case sensitive', function(callback) {
    serviceInstance.count(
      '/searches-and-aggregation/*',
      {
        criteria: {
          'data.custom': { $eq: 'Odd' }
        },
        options: {}
      },
      function(e, result) {
        if (e) return callback(e);
        expect(result.data.value).to.be(1);
        callback();
      }
    );
  });

  it('tests an aggregated search', function(callback) {
    serviceInstance.find(
      '/searches-and-aggregation/*',
      {
        criteria: {
          'data.group': {
            $eq: 'odd'
          }
        },
        options: {
          aggregate: [
            {
              $group: {
                _id: '$data.custom',
                total: {
                  $sum: '$data.id'
                }
              }
            },
            { $sort: { total: 1 } }
          ]
        }
      },
      function(e, result) {
        if (e) return callback(e);
        expect(result.data.value.length).to.be(4);
        expect(result.data.value).to.eql([
          {
            _id: 'Odd',
            total: 1
          },
          {
            _id: 'ODD',
            total: 5
          },
          {
            _id: 'odD',
            total: 7
          },
          {
            _id: 'odd',
            total: 12
          }
        ]);
        callback();
      }
    );
  });

  it('tests an aggregated search with a case-insensitive collation', function(callback) {
    serviceInstance.find(
      '/searches-and-aggregation/*',
      {
        criteria: {
          'data.group': {
            $eq: 'odd'
          }
        },
        options: {
          aggregate: [
            {
              $group: {
                _id: '$data.custom',
                total: {
                  $sum: '$data.id'
                }
              }
            }
          ],
          collation: {
            locale: 'en_US',
            strength: 1
          }
        }
      },
      function(e, result) {
        if (e) return callback(e);
        expect(result.data.value.length).to.be(1);
        expect(result.data.value).to.eql([
          {
            _id: 'Odd',
            total: 25
          }
        ]);
        callback();
      }
    );
  });
});
