Apache Ignite Kafka Streamer Module
-----------------------------------

Apache Ignite Kafka Streamer module provides streaming from Kafka to Ignite cache.

There are two ways this can be achieved:
- importing Kafka Streamer module in your Maven project and instantiate KafkaStreamer for data streaming;
- using Kafka Connect functionality.

Below are the details.

## Importing Ignite Kafka Streamer Module In Maven Project

If you are using Maven to manage dependencies of your project, you can add Kafka module
dependency like this (replace '${ignite.version}' with actual Ignite version you are
interested in):

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-kafka</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>


## Streaming Data to Ignite via Kafka Connect

Sink Connector will help you export data from Kafka to Ignite cache. It polls data from Kafka topics and writes it to the user-specified cache.
For more information on Kafka Connect, see [Kafka Documentation](http://kafka.apache.org/documentation.html#connect).

Connector can be found in 'optional/ignite-kafka.' It and its dependencies have to be on the classpath of a Kafka running instance,
as described in the following subsection.

### Setting up and Running

1. Put the following jar files on Kafka's classpath
- ignite-kafka-connect-x.x.x-SNAPSHOT.jar
- ignite-core-x.x.x-SNAPSHOT.jar
- ignite-spring-x.x.x-SNAPSHOT.jar
- geronimo-jcache_1.0_spec-1.0-alpha-1.jar
- spring-aop-4.1.0.RELEASE.jar
- spring-beans-4.1.0.RELEASE.jar
- spring-context-4.1.0.RELEASE.jar
- spring-core-4.1.0.RELEASE.jar
- spring-expression-4.1.0.RELEASE.jar
- commons-logging-1.1.1.jar

2. Prepare worker configurations, e.g.,
```
bootstrap.servers=localhost:9092

key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false

internal.key.converter=org.apache.kafka.connect.storage.StringConverter
internal.value.converter=org.apache.kafka.connect.storage.StringConverter
internal.key.converter.schemas.enable=false
internal.value.converter.schemas.enable=false

offset.storage.file.filename=/tmp/connect.offsets
offset.flush.interval.ms=10000
```

3. Prepare connector configurations, e.g.,
```
# connector
name=string-ignite-connector
connector.class=org.apache.ignite.stream.kafka.connect.IgniteSinkConnector
tasks.max=2
topics=testTopic1,testTopic2

# cache
cacheName=cache1
cacheAllowOverwrite=true
igniteCfg=/some-path/ignite.xml
```
where 'cacheName' is the name of the cache you specify in '/some-path/ignite.xml' and the data from 'testTopic1,testTopic2'
will be pulled and stored. 'cacheAllowOverwrite' is set to true if you want to enable overwriting existing values in cache.
You can also set 'cachePerNodeDataSize' and 'cachePerNodeParOps' to adjust per-node buffer and the maximum number
of parallel stream operations for a single node.

See example-ignite.xml in tests for a simple cache configuration file example.

4. Start connector, for instance, as follows,
```
./bin/connect-standalone.sh myconfig/connect-standalone.properties myconfig/ignite-connector.properties
```

## Checking the Flow

To perform a very basic functionality check, you can do the following,

1. Start Zookeeper
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```

2. Start Kafka server
```
bin/kafka-server-start.sh config/server.properties
```

3. Provide some data input to the Kafka server
```
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test --property parse.key=true --property key.separator=,
k1,v1
```

4. Start the connector. For example,
```
./bin/connect-standalone.sh myconfig/connect-standalone.properties myconfig/ignite-connector.properties
```

5. Check the value is in the cache. For example, via REST,
```
http://node1:8080/ignite?cmd=size&cacheName=cache1
```

## Streaming Cache Event Data to Kafka via Kafka Connect

Source connector enables listening to Ignite cache events and, upon filtering, stream them to Kafka.

Connector can be found in 'optional/ignite-kafka.' It and its dependencies have to be on the classpath of a Kafka running instance,
as described in the following subsection.

### Setting up and Running

1. Put the following jar files on Kafka's classpath
- ignite-kafka-connect-x.x.x-SNAPSHOT.jar
- ignite-core-x.x.x-SNAPSHOT.jar
- geronimo-jcache_1.0_spec-1.0-alpha-1.jar
- ignite-spring-x.x.x-SNAPSHOT.jar
- spring-aop-4.1.0.RELEASE.jar
- spring-beans-4.1.0.RELEASE.jar
- spring-context-4.1.0.RELEASE.jar
- spring-core-4.1.0.RELEASE.jar
- spring-expression-4.1.0.RELEASE.jar
- commons-logging-1.1.1.jar

2. Prepare worker configurations, e.g.,
```
bootstrap.servers=localhost:9092

key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.ignite.stream.kafka.connect.serialization.CacheEventConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false

internal.key.converter=org.apache.kafka.connect.storage.StringConverter
internal.value.converter=org.apache.kafka.connect.storage.StringConverter
internal.key.converter.schemas.enable=false
internal.value.converter.schemas.enable=false

offset.storage.file.filename=/tmp/connect.offsets
offset.flush.interval.ms=10000
```

Note that the current implementation ignores key and schema of Kafka Connect, and stores marshalled cache events
using org.apache.ignite.stream.kafka.connect.serialization.CacheEventConverter.

3. Prepare connector configurations, e.g.,
```
# connector
name=ignite-src-connector
connector.class=org.apache.ignite.stream.kafka.connect.IgniteSourceConnector
tasks.max=2

# cache
topicNames=testTopic1,testTopic2
cacheEvts=put,removed
## if you decide to filter remotely (recommended)
cacheFilterCls=MyFilter
cacheName=cache1
igniteCfg=/some-path/ignite.xml
```
where 'cacheName' is the name of the cache you specify in '/some-path/ignite.xml' and the data from 'testTopic1,testTopic2'
will be pulled and stored. Also consider using 'evtBufferSize' and 'evtBatchSize' for tuning the internal queue
used to safely transfer data from Ignite cache to Kafka.

The following cache events can be specified in the connector configurations:
- CREATED
- DESTROYED
- PUT
- READ
- REMOVED
- LOCKED
- UNLOCKED
- SWAPPED
- UNSWAPPED
- EXPIRED

For a simple cache configuration file example, see example-ignite.xml in tests.

4. Start the connector, as described in [Kafka Documentation](http://kafka.apache.org/documentation.html#connect).
