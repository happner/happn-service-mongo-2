var path = require('path');
/*
Starts a bunch of concurrent processes that try and cause collisions by updating records on the same path.
This is in an effort to be sure we are dealing with mongos astonishing upsert:true on a unique index issue.
*/
describe('integration/' + require('path').basename(__filename) + '\n', function() {
  this.timeout(20000);

  var async = require('async');
  var testId = require('shortid').generate();
  var kid = require('child_process');

  var KIDS_COUNT = 10;
  var CONCURRENT_COUNT = 1000;

  var testKids = [];

  //sorry about the inappropriate humour, I am referring to baby goats of course.
  before('should fork the kids...', function(done) {
    async.times(
      KIDS_COUNT,
      function(time, timeCB) {
        var testKid = kid.fork(
          path.resolve(__dirname, '..', '__fixtures', 'concurrent_upserts_proc')
        );
        testKids.push(testKid);
        testKid.on('message', function(message) {
          if (message.type != 'startup') return;
          if (message.state == 'startup-error') return timeCB(new Error(message.error));
          timeCB();
        });
        testKid.send({ instruction: 'startup' });
      },
      done
    );
  });

  //mwahahahaha!
  after('should kill the kids...', function(done) {
    async.times(
      KIDS_COUNT,
      function(time, timeCB) {
        testKids[time].kill();
        timeCB();
      },
      done
    );
  });

  it('causes kids to go crazy and all attempt to upsert to the same paths in parallel - we then listen on the kids finished message and check for concurrency issues', function(done) {
    async.times(
      KIDS_COUNT,
      function(time, timeCB) {
        testKids[time].on('message', function(message) {
          if (message.type != 'went crazy') return;
          if (!message.ok) {
            console.log('CONCURRENCY TEST FAILED');
            console.log('-----------------------');
            return timeCB(
              new Error(
                'failed to do concurrent updates to the db, failure count: ' +
                  message.failures.length
              )
            );
          }
          timeCB();
        });
        testKids[time].send({
          instruction: 'go crazy',
          testId: testId,
          concurrent_attempts: CONCURRENT_COUNT
        });
      },
      done
    );
  });
});
