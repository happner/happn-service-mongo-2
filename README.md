[![npm](https://img.shields.io/npm/v/happn-service-mongo-2.svg)](https://www.npmjs.com/package/happn-service-mongo-2) [![Build Status](https://travis-ci.org/happner/happn-service-mongo-2.svg?branch=master)](https://travis-ci.org/happner/happn-service-mongo-2) [![Coverage Status](https://coveralls.io/repos/happner/happn-service-mongo-2/badge.svg?branch=master&service=github)](https://coveralls.io/github/happner/happn-service-mongo-2?branch=master) [![David](https://img.shields.io/david/happner/happn-service-mongo-2.svg)]()

<img src="https://raw.githubusercontent.com/happner/happner-website/master/images/HAPPN%20Logo%20B.png" width="300"></img>

Introduction
-------------------------

### installing mongo and redis on your local machine - for testing:
```bash
# mongo latest
docker pull mongo

docker run -p 27017:27017 -d mongo

# redis
docker pull redis

docker run -p 6379:6379 -d redis
```

### Two configuration options:

```javascript
config = {
  // name of collection where happn/happner stores data
  collection: 'collectioName',
  
  // database housing the collection
  url: 'mongodb://127.0.0.1:27017/databaseName'
}
```


Getting started
---------------------------

### Using this plugin from happner.

```bash
npm install happner happn-service-mongo --save
```

See [happner](https://github.com/happner/happner-2) for full complement of config.

```javascript
var Happner = require('happner');

var config = {
  happn: {
    plugin: 'happn-service-mongo',
    config: {
      collection: 'happner',
      url: 'mongodb://127.0.0.1:27017/happner'
    }
  }
};

Happner.create(config)

  .then(function(server) {
    // ...
  })

  .catch(function(error) {
    console.error(error.stack);
    process.exit(1);
  });
```

### Using this plugin from happn.

```bash
npm install happn happn-service-mongo --save
```

See [happn](https://github.com/happner/happn-3) for full complement of config.

```javascript
var Happn = require('happn');

var config = {
  services: {
    data: {
      path: 'happn-service-mongo',
      config: {
        collection: 'happn',
        url: 'mongodb://127.0.0.1:27017/happn'
      }
    }
  }
};

Happn.service.create(config)

  .then(function(server) {
    //...
  })

  .catch(function(error) {
    console.error(error.stack);
    process.exit(1);
  });

```

##release 0.1.0

 - ability to partition db's and collections by path
 - allow for update or findAndModify depending on options
 - embedded LRU cache, that can use redis pubsub to share state around
