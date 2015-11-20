Apache Ignite Flume Streamer Module
------------------------

Apache Ignite Flume Streamer module provides streaming from Flume to Ignite cache.

To enable Flume Streamer module when starting a standalone node, move 'optional/ignite-Flume' folder to
'libs' folder before running 'ignite.{sh|bat}' script. The content of the module folder will
be added to classpath in this case.

Importing Ignite Flume Streamer Module In Maven Project
-------------------------------------

If you are using Maven to manage dependencies of your project, you can add JCL module
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
            <artifactId>ignite-Flume</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>


## Setting up and running

1. Create a transformer by implementing EventTransformer interface.
2. Build it and copy to ${FLUME_HOME}/plugins.d/ignite-sink/lib.
3. Copy other Ignite-related jar files to ${FLUME_HOME}/plugins.d/ignite-sink/libext to have them as shown below.

```
plugins.d/
`-- ignite
    |-- lib
    |   `-- ignite-flume-transformer-x.x.x.jar <-- your jar
    `-- libext
        |-- cache-api-1.0.0.jar
        |-- ignite-core-x.x.x.jar
        |-- ignite-flume-x.x.x.jar
        |-- ignite-spring-x.x.x.jar
        |-- spring-aop-4.1.0.RELEASE.jar
        |-- spring-beans-4.1.0.RELEASE.jar
        |-- spring-context-4.1.0.RELEASE.jar
        |-- spring-core-4.1.0.RELEASE.jar
        `-- spring-expression-4.1.0.RELEASE.jar
```

4. In Flume configuration file, specify Ignite configuration XML file's location with cache properties
(see [Apache Ignite](https://apacheignite.readme.io/) with cache name specified for cache creation,
cache name (same as in Ignite configuration file), your EventTransformer's implementation class,
and, optionally, batch size (default -- 100).

```
# Describe the sink
a1.sinks.k1.type = org.apache.ignite.stream.flume.IgniteSink
a1.sinks.k1.igniteCfg = /some-path/ignite.xml
a1.sinks.k1.cacheName = testCache
a1.sinks.k1.eventTransformer = my.company.MyEventTransformer
a1.sinks.k1.batchSize = 100
```

After specifying your source and channel (see Flume's docs), you are ready to run a Flume agent.
