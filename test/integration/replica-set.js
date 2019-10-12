var filename = require('path').basename(__filename);

describe('integration/' + filename + '\n', function() {
  this.timeout(20000);

  var expect = require('expect.js');

  var service = require('../../index');

  var fullUrl =
    'mongodb://username:password@127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/happn?replicaSet=test-set&ssl=true&authSource=admin';

  it('should handle a url as is if a dbname is specified', function() {
    var config = {
      url: fullUrl
    };

    var serviceInstance = new service(config);
    expect(serviceInstance.config.url).to.equal(config.url);
  });

  it('should build the url if a db is specified in the options', function() {
    var config = {
      url: 'mongodb://username:password@127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019',
      database: 'happn-test'
    };

    var serviceInstance = new service(config);
    expect(serviceInstance.config.url).to.equal(config.url + '/' + config.database);
  });

  it('should build the url if a db is specified in the options and there are options in the url', function() {
    var config = {
      url:
        'mongodb://username:password@127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019?replicaSet=test-set&ssl=true&authSource=admin',
      database: 'happn-test'
    };

    var serviceInstance = new service(config);
    expect(serviceInstance.config.url).to.equal(
      'mongodb://username:password@127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/happn-test?replicaSet=test-set&ssl=true&authSource=admin'
    );
  });
});
