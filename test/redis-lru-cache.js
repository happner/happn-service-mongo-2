describe('stress', function() {

  this.timeout(20000);

  var expect = require('expect.js');

  var service = require('../index');

  var serviceInstance = new service();

  var async = require('async');

  var config = {

    url:'mongodb://127.0.0.1:27017',

    database:'happn-mongo-cache-lru',

    collection:'happn-mongo-cache-lru-coll',

    datastores: [
      {
        name: 'default',
        isDefault: true,
        database:'happn-mongo-cache-default'
      },
      {
        name: 'historical',
        patterns: [
          '/history/*'
        ],
        url:'mongodb://127.0.0.1:27017',
        collection:'happn-mongo-cache-history'
      }
    ]
  };

  var clearCollections = function(callback){

    serviceInstance.remove('/history/*', {}, callback);
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

  it('initializes the cache', function(callback) {

  });

  it('pushes data into the cache', function(callback) {

  });

  it('gets data out of the cache', function(callback) {

  });

  it('removes an item from the cache', function(callback) {

  });

  it('clears the cache', function(callback) {

  });

  it('starts a remote instance of the cache, and performs a series of operations, checks the local cache picks them up', function(callback) {

  });

});