var async = require('async');
var service = require('../../index');

var config = {
  url: 'mongodb://127.0.0.1:27017/happn'
};

var serviceInstance = new service(config);

function startup() {
  serviceInstance.initialize(function(e) {
    if (e)
      return process.send({
        state: 'startup-error',
        error: e.message,
        type: 'startup'
      });

    serviceInstance.happn = {
      services: {
        utils: {
          wildcardMatch: function(pattern, matchTo) {
            var regex = new RegExp(pattern.replace(/[*]/g, '.*'));
            var matchResult = matchTo.match(regex);
            if (matchResult) return true;
            return false;
          }
        }
      }
    };
    process.send({ state: 'startup-ok', type: 'startup' });
  });
}

function goCrazy(instruction) {
  var failures = [];

  async.timesSeries(
    instruction.concurrent_attempts,
    function(time, timeCB) {
      serviceInstance.upsert(
        '/upsert/' + instruction.testId + '/' + time,
        { data: { test: 'data' } },
        function(e) {
          if (e) failures.push(e.message);
          timeCB();
        }
      );
    },
    function() {
      process.send({
        ok: failures.length == 0,
        failures: failures,
        type: 'went crazy'
      });
    }
  );
}

process.on('message', function(message) {
  if (message.instruction == 'startup') return startup();
  if (message.instruction == 'go crazy') return goCrazy(message);
});
