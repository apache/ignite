# Apache Ignite

<a href="https://ignite.apache.org/"><img src="https://github.com/apache/ignite-website/blob/master/assets/images/apache_ignite_logo.svg" hspace="20"/></a>


## Ignite 2.x and 3.x

* This repository is Ignite 2
* See [apache/ignite-3](https://github.com/apache/ignite-3) for Ignite 3
* Both versions are actively developed

## What is Apache Ignite?

Apache Ignite is a distributed database for high-performance computing with in-memory speed.

<p align="center">
    <a href="https://ignite.apache.org">
        <img src="https://github.com/apache/ignite-website/blob/master/docs/2.9.0/images/ignite_clustering.png" width="400px"/>
    </a>
</p>

* [Technical Documentation](https://ignite.apache.org/docs/latest/)
* [JavaDoc](https://ignite.apache.org/releases/latest/javadoc/)
* [C#/.NET APIs](https://ignite.apache.org/releases/latest/dotnetdoc/api/)
* [C++ APIs](https://ignite.apache.org/releases/latest/cppdoc/)




## Mongodb Backend for Vector Engine
    
Ignite can be used as a Mongo db server, it is fast and distributed, it supports the mongodb3.6 protocol and most mongdb commands. Be default, if ignite instance name is admin, Ignite will start Mongo server automatic, Other wise, You should add MongoPluginConfiguration in IgniteConfiguration.plugins.

It also supports vector search:

    ```
    dbname = 'graph'
    mongo_client = MongoClient("mongodb://127.0.0.1:27018/"+dbname)
    
    collection = mongo_client[dbname][collection_name]
    
    collection_name = 'test_embedding'
    collection = mongo_client[dbname][collection_name]
    collection.create_index([('text','text')])
    collection.create_index([('embedding','knnVector')])
    embeddings = embedding_function
    vectorstore = MongoDBAtlasVectorSearch(collection, embeddings)

    vectorstore.add_texts(['中国的首都','日本的首都','西安','南京','日本海','蒙古大草原','北京','东京'])

    result = vectorstore.similarity_search_with_score('北京',k=4)
    ```

## Text Search

Based on the Mongodb protocol, Ignite supports text search, like Mongodb text search.


## Gremlin Server
Ignite can be used as a Gremlin server, it is fast and distributed, it supports the gremlin3.5+ protocol and all gremlin step. Be default, if ignite instance name is graph, Ignite will start Gremlin server automatic,Other wise, You should add GremlinPluginConfiguration in IgniteConfiguration.plugins.

	```
	import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
	import de.kp.works.ignite.gremlin.sql.IgniteGraphTraversalSource
	// gremlin-core module
	import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

	g = traversal().withRemote(DriverRemoteConnection.using("localhost", 8182));
	// addV will create record in ignite [graph]_vertex table
	g.addV('node').property('id','test2').property('name','测试二').next()

	// Ignite extensions:
	g = traversal(IgniteGraphTraversalSource.class).withRemote(DriverRemoteConnection.using("localhost", 8182))
	// addDoc will create record in ignite [graph]_test table
	v1 = g.addDoc('test').property('id','test2').property('name','测试二').next()
	// addIndex set field name as ignite entity index
	g.addIndex('test','name',false)
	g.selectQuery('test',null)
	// use sql to query vertexes
	g.selectQuery('test',"select count(*) from graph_test ").valueMap().next()
	g.selectQuery('test',"select count(*) from graph_test where name=? ",'测试二').valueMap().next()
	```


## Redis
Ignite can also be used as a distributed redis server. To execute below script, run an Ignite instance with 'redis-ignite-internal-cache-0' cache specified and configured. 

    ```
    import redis    

    r = redis.StrictRedis(host='localhost', port=11211, db=0)

    # set entry.
    r.set('k1', 1)

    # check.
    print('Value for "k1": %s' % r.get('k1'))

    # change entry's value.
    r.set('k1', 'new_val')

    # check.
    print('Value for "k1": %s' % r.get('k1'))
    ```

## Memcached 

Ignite is also available as a distributed memcached server.

php code:

    ```

    // Create client instance.
    $client = new Memcache();

    // Set localhost and port (set to correct values).
    $client->addServer("localhost", 11211, 1);

    // Force client to use binary protocol.
    //$client->setOption(Memcached::OPT_BINARY_PROTOCOL, true);

    // Put entry to cache.
    if ($client->set("key", "val"))
        echo ">>> Successfully put entry in cache.\n";

    // Check entry value.
    echo(">>> Value for 'key': " . $client->get("key") . "\n");

    ```


## ElasticSearch Server

Ignite replaces elasticsearch as a backend search engine implementation

Ignite cache corresponds to the index of elasticsearch.
The entity table corresponds to the type of elasticsearch.
When cache and table are one-to-one, the type parameter is not required(like es7.0+)

Using the http rest interface, it can emulate some of the functionality of elasticsearch, and it is distributed, across data centers.

## Distributed File System
Ignite has an distributed filesystem interface to work with files in memory/disk. IGFS is the acronym of Ignite distributed file system. 
IGFS provides APIs to perform the following operations:

- CRUD (Create, Read, Update, and Delete Files/Directories) file operations
- Perform MapReduce; it sits on top of Hadoop File system (HDFS) to accelerate Hadoop processing
- File caching and eviction

When ignite-rest-http enabled, vistit /filemanger to manager IGFS files.

## Multi-Tier Storage

Apache Ignite is designed to work with memory, disk, and Intel Optane as active storage tiers. The memory tier allows using DRAM and Intel® Optane™ operating in the Memory Mode for data storage and processing needs. The disk tier is optional with the support of two options -- you can persist data in an external database or keep it in the Ignite native persistence. SSD, Flash, HDD, or Intel Optane operating in the AppDirect Mode can be used as a storage device.

[Read More](https://ignite.apache.org/arch/multi-tier-storage.html)

## Ignite Native Persistence

Even though Apache Ignite is broadly used as a caching layer on top of external databases, it comes with its native persistence - a distributed, ACID, and SQL-compliant disk-based store. The native persistence integrates into the Ignite multi-tier storage as a disk tier that can be turned on to let Ignite store more data on disk than it can cache in memory and to enable fast cluster restarts.

[Read More](https://ignite.apache.org/arch/persistence.html)

## ACID Compliance
Data stored in Ignite is ACID-compliant both in memory and on disk, making Ignite a **strongly consistent** system. Ignite transactions work across the network and can span multiple servers.

[Read More](https://ignite.apache.org/features/transactions.html)

## ANSI SQL Support
Apache Ignite comes with a ANSI-99 compliant, horizontally scalable, and fault-tolerant SQL engine that allows you to interact with Ignite as with a regular SQL database using JDBC, ODBC drivers, or native SQL APIs available for Java, C#, C++, Python, and other programming languages. Ignite supports all DML commands, including SELECT, UPDATE, INSERT, and DELETE queries as well as a subset of DDL commands relevant for distributed systems.

[Read More](https://ignite.apache.org/features/sql.html)

## High-Performance Computing
High-performance computing (HPC) is the ability to process data and perform complex calculations at high speeds. Using Apache Ignite as a [high-performance compute cluster](https://ignite.apache.org/use-cases/hpc.html), you can turn a group of commodity machines or a cloud environment into a distributed supercomputer of interconnected Ignite nodes. Ignite enables speed and scale by processing records in memory and reducing network utilization with APIs for data and compute-intensive calculations. Those APIs implement the MapReduce paradigm and allow you to run arbitrary tasks across the cluster of nodes.

