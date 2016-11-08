var happn = require('happn-3');
var happn_client = happn.client;

module.exports = {
  happnDependancy:require('happn-3'),
  description:"eventemitter embedded functional tests",
  serviceConfig:{
    secure:true,
    services: {
      data: {
        instance:TEST_GLOBALS.mongoService
      }
    }
  },
  publisherClient:function(happnInstance, callback){

    var config =  {
      username:'_ADMIN',
      password:'happn'
    };

    happnInstance.services.session.localClient(config, callback);

  },
  listenerClient:function(happnInstance, callback){

    var config =  {
      username:'_ADMIN',
      password:'happn'
    };

    happnInstance.services.session.localClient(config, callback);
  }
};