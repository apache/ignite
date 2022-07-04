Supported operations:
* ignite
  * startClient
  * close
  * createCache
* cache
  * get
  * put
  * remove
  * getAndPut
  * getAndRemove
  * getAll
  * putAll
  * removeAll
  * query
    * sql
    * scan
    * index
  * invoke
  * invokeAll
  * lock
* transaction
  * txStart
  * commit
  * rollback


Limitations 

Transactions only for closed load if number of users injected less then number
of actors in Gatling (20 by default).
