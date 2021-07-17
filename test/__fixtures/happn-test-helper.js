const happn = require('happn-3');
const service = happn.service;
const clearMongo = require('./clear-mongo-collection');

class HappnTestHelper {
  constructor(serverConfig) {
    this.serverConfig = serverConfig;
    this.delay = require('await-delay');
  }

  static create(serverConfig) {
    return new HappnTestHelper(serverConfig);
  }

  async clearMongoCollection() {
    return new Promise((resolve, reject) => {
      clearMongo(
        'mongodb://localhost',
        this.serverConfig.services.data.config.datastores[0].collection,
        e => {
          if (e) return reject(e);
          resolve();
        }
      );
    });
  }

  async createService() {
    return new Promise((resolve, reject) => {
      service.create(this.serverConfig, (e, happnInst) => {
        if (e) return reject(e);
        resolve(happnInst);
      });
    });
  }

  async createLocalClient() {
    if (this.serverConfig.secure)
      return new Promise((resolve, reject) => {
        this.service.services.session.localAdminClient((e, instance) => {
          if (e) return reject(e);
          resolve(instance);
        });
      });
    return new Promise((resolve, reject) => {
      this.service.services.session.localClient((e, instance) => {
        if (e) return reject(e);
        resolve(instance);
      });
    });
  }

  async initialize() {
    await this.clearMongoCollection();
    this.service = await this.createService();
    this.listenerclient = await this.createLocalClient();
    this.publisherclient = await this.createLocalClient();
  }

  async tearDown() {
    return new Promise((resolve, reject) => {
      this.service.stop(function(e) {
        if (e) return reject(e);
        resolve();
      });
    });
  }
}

module.exports = HappnTestHelper;
