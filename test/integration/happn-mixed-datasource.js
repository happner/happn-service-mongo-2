describe('happn-tests, mixed datasource', function() {
  this.timeout(5000);
  let fs = require('fs');
  let expect = require('expect.js');
  let async = require('async');
  let mode = 'embedded';
  let test_id;
  let path = require('path');
  var happnTestHelper;

  var publisherclient;
  var listenerclient;

  const TEST_COLLECTION_NAME = 'happn-service-mongo-2-tests';
  const db_path = path.resolve(__dirname.replace('test/integration', '')) + path.sep + 'index.js';
  const db_local_file_path = __dirname + path.sep + 'tmp' + path.sep + 'functional_mixed.nedb';
  const config = {
    services: {
      data: {
        config: {
          datastores: [
            {
              name: 'mongo',
              provider: db_path,
              isDefault: true,
              collection: TEST_COLLECTION_NAME
            },
            {
              name: 'nedb',
              settings: {
                filename: db_local_file_path
              },
              patterns: ['/LOCAL/*']
            }
          ]
        }
      }
    }
  };

  before('should initialize the service and clients', async () => {
    try {
      fs.unlinkSync(db_local_file_path);
    } catch (e) {
      //do nothing
    }
    test_id = Date.now() + '_' + require('shortid').generate();
    happnTestHelper = require('../__fixtures/happn-test-helper').create(config);
    await happnTestHelper.initialize();
    publisherclient = happnTestHelper.publisherclient;
    listenerclient = happnTestHelper.listenerclient;
  });

  after(async () => {
    await happnTestHelper.tearDown();
  });

  var findRecordInDataFile = function(path, filepath, callback) {
    try {
      setTimeout(function() {
        var fs = require('fs'),
          byline = require('byline');
        var stream = byline(fs.createReadStream(filepath, { encoding: 'utf8' }));
        var found = false;

        stream.on('data', function(line) {
          if (found) return;

          var record = JSON.parse(line);

          if (
            record.operation != null &&
            record.operation.operationType === 'UPSERT' &&
            record.operation.arguments[0] === path
          ) {
            found = true;
            stream.end();
            return callback(null, record);
          }
        });

        stream.on('end', function() {
          if (!found) callback(null, null);
        });
      }, 1000);
    } catch (e) {
      callback(e);
    }
  };

  it('the publisher should set local new data', function(callback) {
    try {
      var test_path_end = require('shortid').generate();

      publisherclient.set(
        '/LOCAL/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/' + test_path_end,
        {
          property1: 'property1',
          property2: 'property2',
          property3: 'property3'
        },
        {
          noPublish: true
        },
        function(e, result) {
          if (!e) {
            publisherclient.get(
              '/LOCAL/1_eventemitter_embedded_sanity/' +
                test_id +
                '/testsubscribe/data/' +
                test_path_end,
              null,
              function(e, results) {
                expect(results.property1 == 'property1').to.be(true);
                expect(results.created == results.modified).to.be(true);

                findRecordInDataFile(
                  '/LOCAL/1_eventemitter_embedded_sanity/' +
                    test_id +
                    '/testsubscribe/data/' +
                    test_path_end,
                  db_local_file_path,
                  function(e, record) {
                    if (e) return callback(e);

                    if (!record)
                      return callback('record not found in data file: ' + db_local_file_path);

                    callback();
                  }
                );
              }
            );
          } else callback(e);
        }
      );
    } catch (e) {
      callback(e);
    }
  });

  it('the listener should pick up a single wildcard event, locally', function(callback) {
    try {
      //first listen for the change
      listenerclient.on(
        '/LOCAL/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event/*',
        {
          event_type: 'set',
          count: 1
        },
        function(message) {
          expect(
            listenerclient.state.events[
              '/SET@/LOCAL/1_eventemitter_embedded_sanity/' +
                test_id +
                '/testsubscribe/data/event/*'
            ]
          ).to.be(undefined);
          callback();
        },
        function(e) {
          if (!e) {
            expect(
              listenerclient.state.events[
                '/SET@/LOCAL/1_eventemitter_embedded_sanity/' +
                  test_id +
                  '/testsubscribe/data/event/*'
              ].length
            ).to.be(1);

            //then make the change
            publisherclient.set(
              '/LOCAL/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event/blah',
              {
                property1: 'property1',
                property2: 'property2',
                property3: 'property3'
              },
              null,
              function(e, result) {}
            );
          } else callback(e);
        }
      );
    } catch (e) {
      callback(e);
    }
  });

  it('the publisher should get null for unfound data, exact path', function(callback) {
    var test_path_end = require('shortid').generate();
    publisherclient.get(
      '/LOCAL/1_eventemitter_embedded_sanity/' + test_id + '/unfound/exact/' + test_path_end,
      null,
      function(e, results) {
        ////////////console.log('new data results');

        expect(e).to.be(null);
        expect(results).to.be(null);

        callback(e);
      }
    );
  });

  it('set_multiple, the publisher should set multiple data items, then do a wildcard get to return them', function(callback) {
    var timesCount = 10;

    var testBasePath = '/LOCAL/1_eventemitter_embedded_sanity/' + test_id + '/set_multiple';

    try {
      async.times(
        timesCount,
        function(n, timesCallback) {
          var test_random_path2 = require('shortid').generate();

          publisherclient.set(
            testBasePath + '/' + test_random_path2,
            {
              property1: 'property1',
              property2: 'property2',
              property3: 'property3'
            },
            {
              noPublish: true
            },
            timesCallback
          );
        },
        function(e) {
          if (e) return callback(e);

          listenerclient.get(testBasePath + '/' + '*', null, function(e, results) {
            if (e) return callback(e);

            expect(results.length).to.be(timesCount);

            results.every(function(result) {
              /*
               RESULT SHOULD LOOK LIKE THIS
               { property1: 'property1',
               property2: 'property2',
               property3: 'property3',
               _meta:
               { modified: 1443606046766,
               created: 1443606046766,
               path: '/1_eventemitter_embedded_sanity/1443606046555_VkyH6cE1l/set_multiple/E17kSpqE1l' } }
               */

              expect(result.property1).to.be('property1');
              expect(result._meta.path.indexOf(testBasePath) == 0).to.be(true);

              return true;
            });

            callback();
          });
        }
      );
    } catch (e) {
      callback(e);
    }
  });

  it('should set data, and then merge a new document into the data without overwriting old fields', function(callback) {
    try {
      var test_path_end = require('shortid').generate();

      publisherclient.set(
        '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/merge/' + test_path_end,
        {
          property1: 'property1',
          property2: 'property2',
          property3: 'property3'
        },
        null,
        function(e, result) {
          if (e) return callback(e);

          publisherclient.set(
            '/1_eventemitter_embedded_sanity/' +
              test_id +
              '/testsubscribe/data/merge/' +
              test_path_end,
            {
              property4: 'property4'
            },
            {
              merge: true
            },
            function(e, result) {
              if (e) return callback(e);

              publisherclient.get(
                '/1_eventemitter_embedded_sanity/' +
                  test_id +
                  '/testsubscribe/data/merge/' +
                  test_path_end,
                null,
                function(e, results) {
                  if (e) return callback(e);

                  //////////////console.log('merge get results');
                  //////////////console.log(results);

                  expect(results.property4).to.be('property4');
                  expect(results.property1).to.be('property1');

                  callback();
                }
              );
            }
          );
        }
      );
    } catch (e) {
      callback(e);
    }
  });

  it('should contain the same payload between 2 non-merging consecutive stores', function(done) {
    var object = {
      param1: 10,
      param2: 20
    };
    var firstTimeNonMergeConsecutive;

    listenerclient.on(
      'setTest/nonMergeConsecutive',
      {
        event_type: 'set',
        count: 2
      },
      function(message, meta) {
        if (firstTimeNonMergeConsecutive === undefined) {
          firstTimeNonMergeConsecutive = message;
        } else {
          expect(message).to.eql(firstTimeNonMergeConsecutive);
          done();
        }
      },
      function(err) {
        expect(err).to.not.exist;
        publisherclient.set('setTest/nonMergeConsecutive', object, {}, function(err) {
          expect(err).to.not.be.ok();
          publisherclient.set('setTest/nonMergeConsecutive', object, {}, function(err) {
            expect(err).to.not.be.ok();
          });
        });
      }
    );
  });

  it('should contain the same payload between a merge and a normal store for first store', function(done) {
    var object = { param1: 10, param2: 20 };
    var firstTime = true;

    listenerclient.on(
      'mergeTest/object',
      { event_type: 'set', count: 2 },
      function(message, meta) {
        expect(message).to.eql(object);
        if (firstTime) {
          firstTime = false;
          return;
        }
        done();
      },
      function(err) {
        expect(err).to.not.be.ok();
        publisherclient.set('mergeTest/object', object, { merge: true }, function(err) {
          expect(err).to.not.be.ok();
          publisherclient.set('mergeTest/object', object, { merge: true }, function(err) {
            expect(err).to.not.be.ok();
          });
        });
      }
    );
  });

  it('should search for a complex object', function(callback) {
    var test_path_end = require('shortid').generate();
    var complex_obj = {
      regions: ['North', 'South'],
      towns: ['North.Cape Town'],
      categories: ['Action', 'History'],
      subcategories: ['Action.angling', 'History.art'],
      keywords: ['bass', 'Penny Siopis'],
      field1: 'field1'
    };
    var criteria1 = {
      $or: [
        {
          regions: {
            $in: ['North', 'South', 'East', 'West']
          }
        },
        {
          towns: {
            $in: ['North.Cape Town', 'South.East London']
          }
        },
        {
          categories: {
            $in: ['Action', 'History']
          }
        }
      ],
      keywords: {
        $in: ['bass', 'Penny Siopis']
      }
    };

    var options1 = {
      sort: {
        field1: 1
      },
      limit: 1
    };

    var criteria2 = null;

    var options2 = {
      fields: {
        towns: 1,
        keywords: 1
      },
      sort: {
        field1: 1
      },
      limit: 2
    };

    publisherclient.set(
      '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/complex/' + test_path_end,
      complex_obj,
      null,
      function(e, put_result) {
        if (e) return callback(e);

        publisherclient.set(
          '/1_eventemitter_embedded_sanity/' +
            test_id +
            '/testsubscribe/data/complex/' +
            test_path_end +
            '/1',
          complex_obj,
          null,
          function(e, put_result) {
            if (e) return callback(e);

            ////////////console.log('searching');
            publisherclient.get(
              '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/complex*',
              {
                criteria: criteria1,
                options: options1
              },
              function(e, search_result) {
                if (e) return callback(e);

                expect(search_result.length == 1).to.be(true);

                publisherclient.get(
                  '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/complex*',
                  {
                    criteria: criteria2,
                    options: options2
                  },
                  function(e, search_result) {
                    if (e) return callback(e);
                    expect(search_result.length == 2).to.be(true);
                    expect(Object.keys(search_result[0]).length).to.be(3);
                    callback(e);
                  }
                );
              }
            );
          }
        );
      }
    );
  });

  it('should search for a complex object by dates', function(callback) {
    var test_path_end = require('shortid').generate();

    var complex_obj = {
      regions: ['North', 'South'],
      towns: ['North.Cape Town'],
      categories: ['Action', 'History'],
      subcategories: ['Action.angling', 'History.art'],
      keywords: ['bass', 'Penny Siopis'],
      field1: 'field1'
    };

    var from = Date.now();
    var to;

    publisherclient.set(
      '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/complex/' + test_path_end,
      complex_obj,
      null,
      function(e, put_result) {
        expect(e == null).to.be(true);

        setTimeout(function() {
          to = Date.now();

          var criteria = {
            '_meta.created': {
              $gte: from,
              $lte: to
            }
          };

          var options = {
            fields: null,
            sort: {
              field1: 1
            },
            limit: 2
          };

          ////////////console.log('searching');
          publisherclient.get(
            '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/complex*',
            {
              criteria: criteria,
              options: options
            },
            function(e, search_result) {
              expect(e == null).to.be(true);

              if (search_result.length == 0) {
                publisherclient.get(
                  '/1_eventemitter_embedded_sanity/' +
                    test_id +
                    '/testsubscribe/data/complex/' +
                    test_path_end,
                  function(e, unmatched) {
                    callback(new Error('no items found in the date range'));
                  }
                );
              } else {
                publisherclient.get(
                  '/1_eventemitter_embedded_sanity/' +
                    test_id +
                    '/testsubscribe/data/complex/' +
                    test_path_end,
                  function(e, unmatched) {
                    callback();
                  }
                );
              }
            }
          );
        }, 300);
      }
    );
  });

  it('should delete some test data', function(callback) {
    try {
      //We put the data we want to delete into the database
      publisherclient.set(
        '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/delete_me',
        {
          property1: 'property1',
          property2: 'property2',
          property3: 'property3'
        },
        {
          noPublish: true
        },
        function(e, result) {
          //We perform the actual delete
          publisherclient.remove(
            '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/delete_me',
            {
              noPublish: true
            },
            function(e, result) {
              expect(e).to.be(null);
              expect(result._meta.status).to.be('ok');

              ////////////////////console.log('DELETE RESULT');
              ////////////////////console.log(result);

              callback();
            }
          );
        }
      );
    } catch (e) {
      callback(e);
    }
  });

  it('the publisher should set new data then update the data', function(callback) {
    try {
      var test_path_end = require('shortid').generate();

      publisherclient.set(
        '1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/' + test_path_end,
        {
          property1: 'property1',
          property2: 'property2',
          property3: 'property3'
        },
        {
          noPublish: true
        },
        function(e, insertResult) {
          expect(e).to.be(null);

          publisherclient.set(
            '1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/' + test_path_end,
            {
              property1: 'property1',
              property2: 'property2',
              property3: 'property3',
              property4: 'property4'
            },
            {
              noPublish: true
            },
            function(e, updateResult) {
              expect(e).to.be(null);
              expect(updateResult._meta.id == insertResult._meta.id).to.be(true);
              callback();
            }
          );
        }
      );
    } catch (e) {
      callback(e);
    }
  });

  it('the listener should pick up a single published event', function(callback) {
    try {
      //first listen for the change
      listenerclient.on(
        '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event',
        {
          event_type: 'set',
          count: 1
        },
        function(message) {
          expect(
            listenerclient.state.events[
              '/SET@/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event'
            ]
          ).to.be(undefined);
          callback();
        },
        function(e) {
          //////////////////console.log('ON HAS HAPPENED: ' + e);

          if (!e) {
            expect(
              listenerclient.state.events[
                '/SET@/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event'
              ].length
            ).to.be(1);
            //////////////////console.log('on subscribed, about to publish');

            //then make the change
            publisherclient.set(
              '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event',
              {
                property1: 'property1',
                property2: 'property2',
                property3: 'property3'
              },
              null,
              function(e, result) {
                ////////////////////////////console.log('put happened - listening for result');
              }
            );
          } else callback(e);
        }
      );
    } catch (e) {
      callback(e);
    }
  });

  //We are testing setting data at a specific path

  it('the publisher should set new data ', function(callback) {
    try {
      var test_path_end = require('shortid').generate();

      publisherclient.set(
        '1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/' + test_path_end,
        {
          property1: 'property1',
          property2: 'property2',
          property3: 'property3'
        },
        null,
        function(e, result) {
          if (!e) {
            publisherclient.get(
              '1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/' + test_path_end,
              null,
              function(e, results) {
                ////////////////////////console.log('new data results');
                ////////////////////////console.log(results);
                expect(results.property1 == 'property1').to.be(true);

                if (mode != 'embedded')
                  expect(results.payload[0].created == results.payload[0].modified).to.be(true);

                callback(e);
              }
            );
          } else callback(e);
        }
      );
    } catch (e) {
      callback(e);
    }
  });

  it('the publisher should set new data then update the data', function(callback) {
    try {
      var test_path_end = require('shortid').generate();

      publisherclient.set(
        '1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/' + test_path_end,
        {
          property1: 'property1',
          property2: 'property2',
          property3: 'property3'
        },
        null,
        function(e, insertResult) {
          expect(e == null).to.be(true);

          publisherclient.set(
            '1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/' + test_path_end,
            {
              property1: 'property1',
              property2: 'property2',
              property3: 'property3',
              property4: 'property4'
            },
            null,
            function(e, updateResult) {
              expect(e == null).to.be(true);
              expect(updateResult._meta._id == insertResult._meta._id).to.be(true);
              callback();
            }
          );
        }
      );
    } catch (e) {
      callback(e);
    }
  });
  //  We set the listener client to listen for a PUT event according to a path, then we set a value with the publisher client.

  it('the listener should pick up a single published event', function(callback) {
    try {
      //first listen for the change
      listenerclient.on(
        '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event',
        {
          event_type: 'set',
          count: 1
        },
        function(message) {
          expect(
            listenerclient.state.events[
              '/SET@/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event'
            ]
          ).to.be(undefined);
          callback();
        },
        function(e) {
          if (!e) {
            expect(
              listenerclient.state.events[
                '/SET@/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event'
              ].length
            ).to.be(1);

            ////////////////////////////console.log('on subscribed, about to publish');

            //then make the change
            publisherclient.set(
              '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event',
              {
                property1: 'property1',
                property2: 'property2',
                property3: 'property3'
              },
              null,
              function(e, result) {
                ////////////////////////////console.log('put happened - listening for result');
              }
            );
          } else callback(e);
        }
      );
    } catch (e) {
      callback(e);
    }
  });

  it('should get using a wildcard', function(callback) {
    var test_path_end = require('shortid').generate();

    publisherclient.set(
      '1_eventemitter_embedded_sanity/' + test_id + '/testwildcard/' + test_path_end,
      {
        property1: 'property1',
        property2: 'property2',
        property3: 'property3'
      },
      null,
      function(e, insertResult) {
        expect(e == null).to.be(true);
        publisherclient.set(
          '1_eventemitter_embedded_sanity/' + test_id + '/testwildcard/' + test_path_end + '/1',
          {
            property1: 'property1',
            property2: 'property2',
            property3: 'property3'
          },
          null,
          function(e, insertResult) {
            expect(e == null).to.be(true);

            publisherclient.get(
              '1_eventemitter_embedded_sanity/' + test_id + '/testwildcard/' + test_path_end + '*',
              null,
              function(e, results) {
                if (e) return callback();

                expect(results.length == 2).to.be(true);
                callback(e);
              }
            );
          }
        );
      }
    );
  });

  it('should get paths', function(callback) {
    var test_path_end = require('shortid').generate();

    publisherclient.set(
      '1_eventemitter_embedded_sanity/' + test_id + '/testwildcard/' + test_path_end,
      {
        property1: 'property1',
        property2: 'property2',
        property3: 'property3'
      },
      null,
      function(e, insertResult) {
        expect(e == null).to.be(true);
        publisherclient.set(
          '1_eventemitter_embedded_sanity/' + test_id + '/testwildcard/' + test_path_end + '/1',
          {
            property1: 'property1',
            property2: 'property2',
            property3: 'property3'
          },
          null,
          function(e, insertResult) {
            expect(e == null).to.be(true);

            publisherclient.getPaths(
              '1_eventemitter_embedded_sanity/' + test_id + '/testwildcard/' + test_path_end + '*',
              function(e, results) {
                expect(results.length == 2).to.be(true);
                callback(e);
              }
            );
          }
        );
      }
    );
  });

  it('the listener should pick up a single delete event', function(callback) {
    //We put the data we want to delete into the database
    publisherclient.set(
      '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/delete_me',
      {
        property1: 'property1',
        property2: 'property2',
        property3: 'property3'
      },
      null,
      function(e, result) {
        //////////////////console.log('did delete set');
        //path, event_type, count, handler, done
        //We listen for the DELETE event
        listenerclient.on(
          '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/delete_me',
          {
            event_type: 'remove',
            count: 1
          },
          function(eventData) {
            //we are looking at the event internals on the listener to ensure our event management is working - because we are only listening for 1
            //instance of this event - the event listener should have been removed
            expect(
              listenerclient.state.events[
                '/REMOVE@/1_eventemitter_embedded_sanity/' +
                  test_id +
                  '/testsubscribe/data/delete_me'
              ]
            ).to.be(undefined);

            //we needed to have removed a single item
            expect(eventData.payload.removed).to.be(1);

            callback();
          },
          function(e) {
            if (!e) return callback(e);

            expect(
              listenerclient.state.events[
                '/REMOVE@/1_eventemitter_embedded_sanity/' +
                  test_id +
                  '/testsubscribe/data/delete_me'
              ].length
            ).to.be(1);

            //We perform the actual delete
            publisherclient.remove(
              '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/delete_me',
              null,
              function(e, result) {}
            );
          }
        );
      }
    );
  });

  it('should unsubscribe from an event', function(callback) {
    var currentListenerId;

    listenerclient.on(
      '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/on_off_test',
      {
        event_type: 'set',
        count: 0
      },
      function(message) {
        //we detach all listeners from the path here
        ////console.log('ABOUT OFF PATH');
        listenerclient.offPath(
          '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/on_off_test',
          function(e) {
            if (e) return callback(new Error(e));

            listenerclient.on(
              '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/on_off_test',
              {
                event_type: 'set',
                count: 0
              },
              function(message) {
                ////console.log('ON RAN');
                ////console.log(message);

                listenerclient.off(currentListenerId, function(e) {
                  if (e) return callback(new Error(e));
                  else return callback();
                });
              },
              function(e, listenerId) {
                if (e) return callback(new Error(e));

                currentListenerId = listenerId;

                publisherclient.set(
                  '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/on_off_test',
                  {
                    property1: 'property1',
                    property2: 'property2',
                    property3: 'property3'
                  },
                  {},
                  function(e, setresult) {
                    if (e) return callback(new Error(e));

                    ////console.log('DID ON SET');
                    ////console.log(setresult);
                  }
                );
              }
            );
          }
        );
      },
      function(e, listenerId) {
        if (e) return callback(new Error(e));

        currentListenerId = listenerId;

        publisherclient.set(
          '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/on_off_test',
          {
            property1: 'property1',
            property2: 'property2',
            property3: 'property3'
          },
          {},
          function(e, setresult) {
            if (e) return callback(new Error(e));
          }
        );
      }
    );
  });

  it('should subscribe to the catch all notification', function(callback) {
    var caught = {};

    this.timeout(10000);
    var caughtCount = 0;

    listenerclient.onAll(
      function(eventData, meta) {
        if (
          meta.action ==
            '/REMOVE@/1_eventemitter_embedded_sanity/' +
              test_id +
              '/testsubscribe/data/catch_all' ||
          meta.action ==
            '/SET@/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/catch_all'
        )
          caughtCount++;

        if (caughtCount == 2) callback();
      },
      function(e) {
        if (e) return callback(e);

        publisherclient.set(
          '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/catch_all',
          {
            property1: 'property1',
            property2: 'property2',
            property3: 'property3'
          },
          null,
          function(e, put_result) {
            publisherclient.remove(
              '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/catch_all',
              null,
              function(e, del_result) {}
            );
          }
        );
      }
    );
  });

  it('should unsubscribe from all events', function(callback) {
    this.timeout(10000);

    var onHappened = false;

    listenerclient.onAll(
      function(message) {
        onHappened = true;
        callback(new Error('this wasnt meant to happen'));
      },
      function(e) {
        if (e) return callback(e);

        listenerclient.on(
          '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/off_all_test',
          {
            event_type: 'set',
            count: 0
          },
          function(message) {
            onHappened = true;
            callback(new Error('this wasnt meant to happen'));
          },
          function(e) {
            if (e) return callback(e);

            listenerclient.offAll(function(e) {
              if (e) return callback(e);

              publisherclient.set(
                '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/off_all_test',
                {
                  property1: 'property1',
                  property2: 'property2',
                  property3: 'property3'
                },
                null,
                function(e, put_result) {
                  if (e) return callback(e);

                  setTimeout(function() {
                    if (!onHappened) callback();
                  }, 3000);
                }
              );
            });
          }
        );
      }
    );
  });

  it('should not publish with noPublish set', function(done) {
    var timeout;
    //first listen for the change
    listenerclient.on(
      '/1_eventemitter_embedded_sanity/' + test_id + '/testNoPublish',
      {
        event_type: 'set',
        count: 1
      },
      function(message) {
        clearTimeout(timeout);
        setImmediate(function() {
          expect(message).to.not.be.ok();
        });
      },
      function(e) {
        expect(e).to.not.be.ok();

        timeout = setTimeout(function() {
          listenerclient.offPath(
            '/1_eventemitter_embedded_sanity/' + test_id + '/testNoPublish',
            function() {
              done();
            }
          );
        }, 1000);
        publisherclient.set(
          '/1_eventemitter_embedded_sanity/' + test_id + '/testNoPublish',
          {
            property1: 'property1',
            property2: 'property2',
            property3: 'property3'
          },
          {
            noPublish: true
          },
          function(e, result) {
            expect(e).to.not.be.ok();
          }
        );
      }
    );
  });

  it('can get using criteria, $regex with params in array', function(done) {
    publisherclient.set(
      '/regex/test/1',
      {
        name: 'Loadtest_123',
        anotherProp: 'anotherPropValue'
      },
      function(e, result) {
        if (e) return done(e);

        var options = {
          fields: {
            name: 1
          }
        };

        var criteria = {
          name: {
            $regex: ['.*loadtest.*', 'i']
          }
        };

        listenerclient.get(
          '/regex/test/*',
          {
            criteria: criteria,
            options: options
          },
          function(e, result) {
            if (e) return done(e);
            expect(result[0].anotherProp).to.be(undefined);
            expect(result[0].name).to.be('Loadtest_123');
            expect(result.length).to.be(1);
            done();
          }
        );
      }
    );
  });

  it('can get using criteria, $regex as string', function(done) {
    publisherclient.set(
      '/regex/test/1',
      {
        name: 'Loadtest_123',
        anotherProp: 'anotherPropValue'
      },
      function(e, result) {
        if (e) return done(e);

        var options = {
          fields: {
            name: 1
          }
        };

        var criteria = {
          name: {
            $regex: '.*Loadtest.*'
          }
        };

        listenerclient.get(
          '/regex/test/*',
          {
            criteria: criteria,
            options: options
          },
          function(e, result) {
            if (e) return done(e);
            expect(result[0].anotherProp).to.be(undefined);
            expect(result[0].name).to.be('Loadtest_123');
            expect(result.length).to.be(1);
            done();
          }
        );
      }
    );
  });

  it('can get using criteria, bad $regex as boolean', function(done) {
    publisherclient.set(
      '/regex/test/1',
      {
        name: 'Loadtest_123',
        anotherProp: 'anotherPropValue'
      },
      function(e, result) {
        if (e) return done(e);

        var options = {
          fields: {
            name: 1
          }
        };

        var criteria = {
          name: {
            $regex: false
          }
        };

        listenerclient.get(
          '/regex/test/*',
          {
            criteria: criteria,
            options: options
          },
          function(e, result) {
            expect(e.toString()).to.be(
              'SystemError: $regex parameter value must be an Array or a string'
            );
            done();
          }
        );
      }
    );
  });
});
