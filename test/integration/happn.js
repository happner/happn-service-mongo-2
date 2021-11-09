describe('happn-tests', function() {
  this.timeout(5000);

  let expect = require('expect.js');
  let async = require('async');
  let mode = 'embedded';
  let test_id;
  let path = require('path');
  var happnTestHelper;

  var publisherclient;
  var listenerclient;

  const db_path = path.resolve(__dirname.replace('test/integration', '')) + path.sep + 'index.js';
  const configs = [
    {
      port: 55000,
      services: {
        data: {
          config: {
            autoUpdateDBVersion: true,
            datastores: [
              {
                name: 'mongo',
                provider: db_path,
                isDefault: true,
                collection: 'happn-service-mongo-2-tests'
              }
            ]
          }
        }
      }
    },
    {
      port: 55002,
      secure: true,
      services: {
        data: {
          config: {
            autoUpdateDBVersion: true,
            datastores: [
              {
                name: 'mongo',
                provider: db_path,
                isDefault: true,
                collection: 'happn-service-mongo-2-tests-secure'
              }
            ]
          }
        }
      }
    }
  ];

  configs.forEach(config => {
    context('running battery of tests', function() {
      before('should initialize the service and clients', async () => {
        test_id = Date.now() + '_' + require('shortid').generate();
        happnTestHelper = require('../__fixtures/happn-test-helper').create(config);
        await happnTestHelper.initialize();
        publisherclient = happnTestHelper.publisherclient;
        listenerclient = happnTestHelper.listenerclient;
      });

      after(async () => {
        await happnTestHelper.tearDown();
      });

      it('the publisher should set new data', function(callback) {
        try {
          let test_path_end = require('shortid').generate();

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
            function(e) {
              if (!e) {
                publisherclient.get(
                  '1_eventemitter_embedded_sanity/' +
                    test_id +
                    '/testsubscribe/data/' +
                    test_path_end,
                  null,
                  function(e, results) {
                    expect(results.property1 == 'property1').to.be(true);
                    expect(results.created == results.modified).to.be(true);

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

      it('the listener should pick up a single wildcard event', function(callback) {
        try {
          //first listen for the change
          listenerclient.on(
            '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event/*',
            {
              event_type: 'set',
              count: 1
            },
            function() {
              expect(
                listenerclient.state.events[
                  '/SET@/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event/*'
                ]
              ).to.be(undefined);
              callback();
            },
            function(e) {
              if (!e) {
                expect(
                  listenerclient.state.events[
                    '/SET@/1_eventemitter_embedded_sanity/' +
                      test_id +
                      '/testsubscribe/data/event/*'
                  ].length
                ).to.be(1);

                //then make the change
                publisherclient.set(
                  '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event/blah',
                  {
                    property1: 'property1',
                    property2: 'property2',
                    property3: 'property3'
                  },
                  null,
                  function() {}
                );
              } else callback(e);
            }
          );
        } catch (e) {
          callback(e);
        }
      });

      it('the publisher should get null for unfound data, exact path', function(callback) {
        let test_path_end = require('shortid').generate();
        publisherclient.get(
          '1_eventemitter_embedded_sanity/' + test_id + '/unfound/exact/' + test_path_end,
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
        let timesCount = 10;

        let testBasePath = '/1_eventemitter_embedded_sanity/' + test_id + '/set_multiple';

        try {
          async.times(
            timesCount,
            function(n, timesCallback) {
              let test_random_path2 = require('shortid').generate();

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
          let test_path_end = require('shortid').generate();

          publisherclient.set(
            '/1_eventemitter_embedded_sanity/' +
              test_id +
              '/testsubscribe/data/merge/' +
              test_path_end,
            {
              property1: 'property1',
              property2: 'property2',
              property3: 'property3'
            },
            null,
            function(e) {
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
                function(e) {
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
        let object = {
          param1: 10,
          param2: 20
        };
        let firstTimeNonMergeConsecutive;

        listenerclient.on(
          'setTest/nonMergeConsecutive',
          {
            event_type: 'set',
            count: 2
          },
          function(message) {
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
        let shortid = require('shortid').generate();

        let object = {
          param1: 10,
          param2: 20
        };
        let firstTime = true;

        listenerclient.on(
          'mergeTest5/object/*',
          {
            event_type: 'set',
            count: 2
          },
          function(message) {
            expect(message).to.eql(object);

            if (firstTime) {
              firstTime = false;
              return;
            }
            done();
          },
          function(err) {
            if (err) return done(err);

            publisherclient.set(
              'mergeTest5/object/' + shortid,
              object,
              {
                merge: true
              },
              function(err) {
                if (err) return done(err);

                publisherclient.set(
                  'mergeTest5/object/' + shortid,
                  object,
                  {
                    merge: true
                  },
                  function(err) {
                    if (err) return done(err);
                  }
                );
              }
            );
          }
        );
      });

      it('should search for a complex object', function(callback) {
        let test_path_end = require('shortid').generate();

        let complex_obj = {
          regions: ['North', 'South'],
          towns: ['North.Cape Town'],
          categories: ['Action', 'History'],
          subcategories: ['Action.angling', 'History.art'],
          keywords: ['bass', 'Penny Siopis'],
          field1: 'field1'
        };

        let criteria1 = {
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

        let options1 = {
          sort: {
            field1: 1
          },
          limit: 1
        };

        let criteria2 = null;

        let options2 = {
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
          '/1_eventemitter_embedded_sanity/' +
            test_id +
            '/testsubscribe/data/complex/' +
            test_path_end,
          complex_obj,
          null,
          function(e) {
            if (e) return callback(e);

            publisherclient.set(
              '/1_eventemitter_embedded_sanity/' +
                test_id +
                '/testsubscribe/data/complex/' +
                test_path_end +
                '/1',
              complex_obj,
              null,
              function(e) {
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
        let test_path_end = require('shortid').generate();

        let complex_obj = {
          regions: ['North', 'South'],
          towns: ['North.Cape Town'],
          categories: ['Action', 'History'],
          subcategories: ['Action.angling', 'History.art'],
          keywords: ['bass', 'Penny Siopis'],
          field1: 'field1'
        };

        let from = Date.now();
        let to;

        publisherclient.set(
          '/1_eventemitter_embedded_sanity/' +
            test_id +
            '/testsubscribe/data/complex/' +
            test_path_end,
          complex_obj,
          null,
          function(e) {
            expect(e == null).to.be(true);

            setTimeout(function() {
              to = Date.now();

              let criteria = {
                '_meta.created': {
                  $gte: from,
                  $lte: to
                }
              };

              let options = {
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
                      function() {
                        callback(new Error('no items found in the date range'));
                      }
                    );
                  } else {
                    publisherclient.get(
                      '/1_eventemitter_embedded_sanity/' +
                        test_id +
                        '/testsubscribe/data/complex/' +
                        test_path_end,
                      function() {
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
            function() {
              //We perform the actual delete
              publisherclient.remove(
                '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/delete_me',
                {
                  noPublish: true
                },
                function(e, result) {
                  expect(e).to.be(null);
                  expect(result._meta.status).to.be('ok');
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
          let test_path_end = require('shortid').generate();

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
                '1_eventemitter_embedded_sanity/' +
                  test_id +
                  '/testsubscribe/data/' +
                  test_path_end,
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
          listenerclient.on(
            '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event',
            {
              event_type: 'set',
              count: 1
            },
            function() {
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
                //then make the change
                publisherclient.set(
                  '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event',
                  {
                    property1: 'property1',
                    property2: 'property2',
                    property3: 'property3'
                  },
                  null,
                  function() {}
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
          let test_path_end = require('shortid').generate();

          publisherclient.set(
            '1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/' + test_path_end,
            {
              property1: 'property1',
              property2: 'property2',
              property3: 'property3'
            },
            null,
            function(e) {
              if (!e) {
                publisherclient.get(
                  '1_eventemitter_embedded_sanity/' +
                    test_id +
                    '/testsubscribe/data/' +
                    test_path_end,
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
          let test_path_end = require('shortid').generate();

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
                '1_eventemitter_embedded_sanity/' +
                  test_id +
                  '/testsubscribe/data/' +
                  test_path_end,
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

      it('the listener should pick up a single published event', function(callback) {
        try {
          //first listen for the change
          listenerclient.on(
            '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event',
            {
              event_type: 'set',
              count: 1
            },
            function() {
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

                //then make the change
                publisherclient.set(
                  '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/event',
                  {
                    property1: 'property1',
                    property2: 'property2',
                    property3: 'property3'
                  },
                  null,
                  function() {}
                );
              } else callback(e);
            }
          );
        } catch (e) {
          callback(e);
        }
      });

      it('should get using a wildcard', function(callback) {
        let test_path_end = require('shortid').generate();

        publisherclient.set(
          '1_eventemitter_embedded_sanity/' + test_id + '/testwildcard/' + test_path_end,
          {
            property1: 'property1',
            property2: 'property2',
            property3: 'property3'
          },
          null,
          function(e) {
            expect(e == null).to.be(true);
            publisherclient.set(
              '1_eventemitter_embedded_sanity/' + test_id + '/testwildcard/' + test_path_end + '/1',
              {
                property1: 'property1',
                property2: 'property2',
                property3: 'property3'
              },
              null,
              function(e) {
                expect(e == null).to.be(true);

                publisherclient.get(
                  '1_eventemitter_embedded_sanity/' +
                    test_id +
                    '/testwildcard/' +
                    test_path_end +
                    '*',
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
        let test_path_end = require('shortid').generate();

        publisherclient.set(
          '1_eventemitter_embedded_sanity/' + test_id + '/testwildcard/' + test_path_end,
          {
            property1: 'property1',
            property2: 'property2',
            property3: 'property3'
          },
          null,
          function(e) {
            expect(e == null).to.be(true);
            publisherclient.set(
              '1_eventemitter_embedded_sanity/' + test_id + '/testwildcard/' + test_path_end + '/1',
              {
                property1: 'property1',
                property2: 'property2',
                property3: 'property3'
              },
              null,
              function(e) {
                expect(e == null).to.be(true);

                publisherclient.getPaths(
                  '1_eventemitter_embedded_sanity/' +
                    test_id +
                    '/testwildcard/' +
                    test_path_end +
                    '*',
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
          function() {
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
                  function() {}
                );
              }
            );
          }
        );
      });

      it('should unsubscribe from an event', function(callback) {
        let currentListenerId;

        listenerclient.on(
          '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/on_off_test',
          {
            event_type: 'set',
            count: 0
          },
          function() {
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
                  function() {
                    listenerclient.off(currentListenerId, function(e) {
                      if (e) return callback(new Error(e));
                      else return callback();
                    });
                  },
                  function(e, listenerId) {
                    if (e) return callback(new Error(e));

                    currentListenerId = listenerId;

                    publisherclient.set(
                      '/1_eventemitter_embedded_sanity/' +
                        test_id +
                        '/testsubscribe/data/on_off_test',
                      {
                        property1: 'property1',
                        property2: 'property2',
                        property3: 'property3'
                      },
                      {},
                      function(e) {
                        if (e) return callback(new Error(e));
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
              function(e) {
                if (e) return callback(new Error(e));
              }
            );
          }
        );
      });

      it('should subscribe to the catch all notification', function(callback) {
        this.timeout(10000);
        let caughtCount = 0;

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
              function() {
                publisherclient.remove(
                  '/1_eventemitter_embedded_sanity/' + test_id + '/testsubscribe/data/catch_all',
                  null,
                  function() {}
                );
              }
            );
          }
        );
      });

      it('should unsubscribe from all events', function(callback) {
        this.timeout(10000);

        let onHappened = false;

        listenerclient.onAll(
          function() {
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
              function() {
                onHappened = true;
                callback(new Error('this wasnt meant to happen'));
              },
              function(e) {
                if (e) return callback(e);

                listenerclient.offAll(function(e) {
                  if (e) return callback(e);

                  publisherclient.set(
                    '/1_eventemitter_embedded_sanity/' +
                      test_id +
                      '/testsubscribe/data/off_all_test',
                    {
                      property1: 'property1',
                      property2: 'property2',
                      property3: 'property3'
                    },
                    null,
                    function(e) {
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
        let timeout;
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
              function(e) {
                expect(e).to.not.be.ok();
              }
            );
          }
        );
      });

      it('increments a value on a path', function(done) {
        let async = require('async');

        let test_string = require('shortid').generate();
        let test_base_url = '/increment/' + test_id + '/' + test_string;

        async.timesSeries(
          10,
          function(time, timeCB) {
            publisherclient.set(
              test_base_url,
              'counter',
              {
                increment: 1,
                noPublish: true
              },
              timeCB
            );
          },
          function(e) {
            if (e) return done(e);

            listenerclient.get(test_base_url, function(e, result) {
              if (e) return done(e);

              expect(result.counter.value).to.be(10);

              done();
            });
          }
        );
      });

      it('the listener can call count for data', function(done) {
        var test_string = require('shortid').generate();
        var test_base_url = '/count_happn/' + test_id + '/set/string/' + test_string;
        publisherclient.set(
          test_base_url,
          test_string,
          {
            noPublish: true
          },
          function(e) {
            expect(e).to.not.exist;
            listenerclient.count(test_base_url, function(e, count) {
              expect(e).to.not.exist;
              expect(count.value).to.eql(1);
              done();
            });
          }
        );
      });

      it('increments a value on a path, multiple gauges', function(done) {
        let async = require('async');

        let test_string = require('shortid').generate();
        let test_base_url = '/increment/' + test_id + '/' + test_string;

        async.timesSeries(
          10,
          function(time, timeCB) {
            publisherclient.set(
              test_base_url,
              'counter-' + time,
              {
                increment: 1,
                noPublish: true
              },
              function(e) {
                timeCB(e);
              }
            );
          },
          function(e) {
            if (e) return done(e);

            listenerclient.get(test_base_url, function(e, result) {
              if (e) return done(e);

              expect(result['counter-0'].value).to.be(1);
              expect(result['counter-1'].value).to.be(1);
              expect(result['counter-2'].value).to.be(1);
              expect(result['counter-3'].value).to.be(1);
              expect(result['counter-4'].value).to.be(1);
              expect(result['counter-5'].value).to.be(1);
              expect(result['counter-6'].value).to.be(1);
              expect(result['counter-7'].value).to.be(1);
              expect(result['counter-8'].value).to.be(1);
              expect(result['counter-9'].value).to.be(1);

              done();
            });
          }
        );
      });

      it('increments a value on a path, convenience method, multiple gauges', function(done) {
        let async = require('async');

        let test_string = require('shortid').generate();
        let test_base_url = '/increment/' + test_id + '/' + test_string;

        async.timesSeries(
          10,
          function(time, timeCB) {
            publisherclient.increment(test_base_url, 'counter-' + time, 1, function(e) {
              timeCB(e);
            });
          },
          function(e) {
            if (e) return done(e);

            listenerclient.get(test_base_url, function(e, result) {
              if (e) return done(e);

              expect(result['counter-0'].value).to.be(1);
              expect(result['counter-1'].value).to.be(1);
              expect(result['counter-2'].value).to.be(1);
              expect(result['counter-3'].value).to.be(1);
              expect(result['counter-4'].value).to.be(1);
              expect(result['counter-5'].value).to.be(1);
              expect(result['counter-6'].value).to.be(1);
              expect(result['counter-7'].value).to.be(1);
              expect(result['counter-8'].value).to.be(1);
              expect(result['counter-9'].value).to.be(1);

              done();
            });
          }
        );
      });

      it('increments a value on a path, convenience method, listens on path receives event', function(done) {
        let test_string = require('shortid').generate();
        let test_base_url = '/increment/convenience/' + test_id + '/' + test_string;

        listenerclient.on(
          test_base_url,
          function(data) {
            expect(data.value).to.be(1);
            expect(data.gauge).to.be('counter');

            done();
          },
          function(e) {
            if (e) return done(e);

            publisherclient.increment(test_base_url, 1, function(e) {
              if (e) return done(e);
            });
          }
        );
      });

      it('increments a value on a path, convenience method with custom gauge and increment, listens on path receives event', function(done) {
        let test_string = require('shortid').generate();
        let test_base_url = '/increment/convenience/' + test_id + '/' + test_string;

        listenerclient.on(
          test_base_url,
          function(data) {
            expect(data.value).to.be(3);
            expect(data.gauge).to.be('custom');

            done();
          },
          function(e) {
            if (e) return done(e);

            publisherclient.increment(test_base_url, 'custom', 3, function(e) {
              if (e) return done(e);
            });
          }
        );
      });

      it('increments and decrements a value on a path, convenience method with custom gauge and increment and decrement, listens on path receives event', function(done) {
        let test_string = require('shortid').generate();
        let test_base_url = '/increment/convenience/' + test_id + '/' + test_string;

        let incrementCount = 0;

        listenerclient.on(
          test_base_url,
          function(data) {
            incrementCount++;

            if (incrementCount == 1) {
              expect(data.value).to.be(3);
              expect(data.gauge).to.be('custom');
            }

            if (incrementCount == 2) {
              expect(data.value).to.be(1);
              expect(data.gauge).to.be('custom');
              done();
            }
          },
          function(e) {
            if (e) return done(e);

            publisherclient.increment(test_base_url, 'custom', 3, function(e) {
              if (e) return done(e);

              publisherclient.increment(test_base_url, 'custom', -2, function(e) {
                if (e) return done(e);
              });
            });
          }
        );
      });

      it('increments a value on a path, convenience method, no counter so defaults to 1, listens on path receives event', function(done) {
        let test_string = require('shortid').generate();
        let test_base_url = '/increment/convenience/' + test_id + '/' + test_string;

        listenerclient.on(
          test_base_url,
          function(data) {
            expect(data.value).to.be(1);
            expect(data.gauge).to.be('counter');

            done();
          },
          function(e) {
            if (e) return done(e);

            publisherclient.increment(test_base_url, function(e) {
              if (e) return done(e);
            });
          }
        );
      });

      it('can page using skip and limit', async () => {
        this.timeout(5000);

        let totalRecords = 100;
        let pageSize = 10;
        let expectedPages = totalRecords / pageSize;
        let indexes = [];

        for (let i = 0; i < totalRecords; i++) indexes.push(i);

        for (let index of indexes) {
          await publisherclient.set('series/horror/' + index, {
            name: 'nightmare on elm street',
            genre: 'horror',
            episode: index
          });
          await publisherclient.set('series/fantasy/' + index, {
            name: 'game of thrones',
            genre: 'fantasy',
            episode: index
          });
        }

        let options = {
          sort: {
            '_meta.created': -1
          },
          limit: pageSize
        };

        let criteria = {
          genre: 'horror'
        };

        let foundPages = [];

        for (let i = 0; i < expectedPages; i++) {
          options.skip = foundPages.length;
          let results = await listenerclient.get('series/*', {
            criteria: criteria,
            options: options
          });
          foundPages = foundPages.concat(results);
        }

        let allResults = await listenerclient.get('series/*', {
          criteria: criteria,
          options: {
            sort: {
              '_meta.created': -1
            }
          }
        });

        expect(allResults.length).to.eql(foundPages.length);
        expect(allResults).to.eql(foundPages);
      });

      it('can get using criteria, $regex with params in array', function(done) {
        publisherclient.set(
          '/regex/test/1',
          {
            name: 'Loadtest_123',
            anotherProp: 'anotherPropValue'
          },
          function(e) {
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
          function(e) {
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
          function(e) {
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
              function(e) {
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
  });
});
