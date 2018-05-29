0.1.0 2016-10-08
----------------
- incorporated redis lru cache

1.0.0 2017-02-08
----------------
- added happn-3 as dev dependancy
- modified to work as a happn-3 data provider

1.0.1 2017-02-15
----------------
- updated happn
- fixed created date not being returned on insert

1.1.0 2017-04-04
----------------
- issue-3 - Allow for replicaSet options
- issue-4 - Allow for ssl in options
- issue-5 - Don't mangle replica set url

1.2.0 2017-06-21
----------------
- issue-7 - Indexes not being created
- updated index to push creation log to database, system also checks if index previously created
- index tests

2.0.0 2018-04-03
----------------
- findAndModify fails on unique index, documented mongo behavior - have retry in place
- removed unnecessary binds on upsert methods
- updated mongodb dependency

2.1.0 2018-05-14
----------------
- increment functionality and tests
- removed archived tests

2.1.1 2018-05-29
----------------
- concurrent upsert tests
- generalized check for unique constraint error on upsert to just 'duplicate key'
