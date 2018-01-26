var DataStore = require('../../lib/datastore');
var async = require('async');
var shortid = require('shortid');

var TIMES = 100000;

var config = {
  url:'mongodb://127.0.0.1:27017',
  internalReconnectionPolicy:false
};

DataStore.create(config, function(e, store){

  if (e) {
    return console.log('create error:::', e);
    process.exit(1);
  }

  async.timesSeries(TIMES, function(time, next){

    store.insert({path: 'loop-test/' + shortid.generate() + '/' + time, test:'data', time:time}, function(e){
      if (e) {
        console.log('insert error:::', time);
        return setTimeout(next, 1000);
      }

      console.log('inserted:::', time);

      setTimeout(next, 1000);
    })
  });
});




