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

1.2.1 2017-06-22
----------------
- updated travis.yml

2.0.0 2017-07-31
----------------
- fixed regex to be less permissive
- fixed reconnection in dataStore to be more resilient to db outages
- when the db is disconnected, requests are no longer queued but immediately rejected



