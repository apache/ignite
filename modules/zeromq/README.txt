Apache Ignite ZeroMQ Streamer Module
------------------------

Apache Ignite ZeroMQ Streamer module provides streaming from ZeroMQ to Ignite cache.

Starting data transfer to Ignite cache can be done with the following steps.

1. Import Ignite ZeroMQ Streamer Module In Maven Project

If you are using Maven to manage dependencies of your project, you can add ZeroMQ module
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
            <artifactId>ignite-zeromq</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>

2. Create an Ignite configuration file (see example-ignite.xml) and make sure it is accessible from the streamer.

3. Create your StreamSingleTupleExtractor or StreamMultipleTupleExtractor, an example can be found in the tests folder
ZeroMqStringSingleTupleExtractor.java. Now it is necessary to add in a streamer
IgniteZeroMqStreamer.setSingleTupleExtractor(...).

4. Create a topology with the streamer and start.
