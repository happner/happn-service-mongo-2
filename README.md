[![npm](https://img.shields.io/npm/v/happn-service-mongo.svg)](https://www.npmjs.com/package/happn-service-mongo) [![Build Status](https://travis-ci.org/happner/happn-service-mongo.svg?branch=master)](https://travis-ci.org/happner/happn-service-mongo) [![Coverage Status](https://coveralls.io/repos/happner/happn-service-mongo/badge.svg?branch=master&service=github)](https://coveralls.io/github/happner/happn-service-mongo?branch=master) [![David](https://img.shields.io/david/happner/happn-service-mongo.svg)]()

<img src="https://raw.githubusercontent.com/happner/happner-website/master/images/HAPPN%20Logo%20B.png" width="300"></img>

Introduction
-------------------------

Two configuration options:

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

See [happner](https://github.com/happner/happner) for full complement of config.

```javascript
var Happner = require('happner');

var config = {
  datalayer: {
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

See [happn](https://github.com/happner/happn) for full complement of config.

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
