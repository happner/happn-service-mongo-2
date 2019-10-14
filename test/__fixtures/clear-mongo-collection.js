var MongoClient = require('mongodb').MongoClient;

module.exports = function(url, collectionName, callback) {
  MongoClient.connect(url, { useNewUrlParser: true, useUnifiedTopology: true }, function(
    err,
    client
  ) {
    if (err) return callback(err);

    let db = client.db(collectionName);
    let collection = db.collection(collectionName);

    collection.drop(function(err) {
      if (err && err.message != 'ns not found')
        if (err) console.log('error clearing mongodb: ' + err.message);
      client.close(callback);
    });
  });
};
