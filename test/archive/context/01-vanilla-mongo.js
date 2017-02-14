var happn = require('happn-3');
var happn_client = happn.client;

module.exports = {
  happnDependancy:require('happn-3'),
  description:"eventemitter embedded functional tests",
  serviceConfig:{
    __noDecouple:true,
    services: {
      data: {
        instance:TEST_GLOBALS.mongoService
      }
    }
  },
  publisherClient:function(happnInstance, callback){

    happnInstance.services.session.localClient(callback);
  },
  listenerClient:function(happnInstance, callback){

    happnInstance.services.session.localClient(callback);
  }
};