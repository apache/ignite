Apache Ignite In-Memory Database and Caching Platform
=====================================================

Ignite is a memory-centric distributed database, caching, and processing platform for transactional, analytical,
and streaming workloads delivering in-memory speeds at petabyte scale.

The main feature set of Ignite includes:
* Memory-Centric Storage
* Advanced Clustering
* Distributed Key-Value
* Distributed SQL
* Compute Grid
* Service Grid
* Distributed Data Structures
* Distributed Messaging
* Distributed Events
* Streaming & CEP

For information on how to get started with Apache Ignite please visit:

    http://apacheignite.readme.io/docs/getting-started


You can find Apache Ignite documentation here:

    http://apacheignite.readme.io/docs

Fulltext search with sql
==========================
start CacheClientBinaryQueryExample.java
then can execute these sql: 

<pre>
CREATE ALIAS IF NOT EXISTS FTL_INIT FOR "org.apache.ignite.cache.FullTextLucene.init"; 

SELECT FTL_INIT(); 

SELECT * FROM FTL_SEARCH('CacheClientBinaryQueryExampleEmployees','EMPLOYEE','TX', 0, 0); 

SELECT e.name,e.street FROM FTL_SEARCH('CacheClientBinaryQueryExampleEmployees','EMPLOYEE','TX', 0, 0) f ,"CacheClientBinaryQueryExampleEmployees".EMPLOYEE e where f._key=e._key 


above sql equals below which row contain table EMPLOYEE fields: 

SELECT e.name,e.street FROM FTL_SEARCH_DATA('CacheClientBinaryQueryExampleEmployees','EMPLOYEE','TX', 0, 0) e;

</pre>

TextQuery with filter
=========================

 see examples/org.apache.ignite.examples.datagrid.CacheQueryExample.java
    https://github.com/junphine/ignite/blob/master/examples/src/main/java/org/apache/ignite/examples/datagrid/CacheQueryExample.java 

```  
 IgniteBiPredicate<AffinityKey, Person> filter = new IgniteBiPredicate<AffinityKey, Person>() {
 
            @Override public boolean apply(AffinityKey key, Person person) {            
                return person.salary > 1000;
            }            
        };  
          
 new TextQuery<Long, Person>(Person.class, "Master",filter)
 
 
```

 
user define function
===================== 
 Support user define function use CREATE ALIAS  like H2.
 CREATE ALIAS <FUNC_NAME> FOR "<package.Class.staticMethod>"
 
 
 
 Elasticsearch Restful Supported  
=======================  
 
 Support some elasticsearch restfull interface.
 such as: _search,_all,_put,_batch  
 
 
 MongoDb Backend Supported   
=======================
 
 Support use mongodb client to connect ignite.
 Now ignite support redis,memcache,mongodb,elasticsearch
 see ignite-mongodb-realy
 
 
 
