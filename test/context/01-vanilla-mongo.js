var happn = require('happn')
var happn_client = happn.client;

module.exports = {
  happnDependancy:require('happn'),
  description:"eventemitter embedded functional tests",
  serviceConfig:{
    services: {
      data: {
        instance:TEST_GLOBALS.mongoService
      }
    }
  },
  publisherClient:function(happnInstance, callback){
    var config =  {
      plugin: happn.client_plugins.intra_process,
      context: happnInstance
    };
    happn_client.create(config, callback);
  },
  listenerClient:function(happnInstance, callback){
    var config =  {
      plugin: happn.client_plugins.intra_process,
      context: happnInstance
    };
    happn_client.create(config, callback);
  }
};