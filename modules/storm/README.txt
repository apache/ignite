Apache Ignite Storm Streamer Module
-----------------------------------

Apache Ignite Storm Streamer module provides streaming via Storm to Ignite cache.

Starting data transfer to Ignite cache can be done with the following steps.

1. Import Ignite Storm Streamer Module In Maven Project

If you are using Maven to manage dependencies of your project, you can add Storm module
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
            <artifactId>ignite-storm</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>

2. Create an Ignite configuration file (see example-ignite.xml) and make sure it is accessible from the streamer.

3. Make sure your key-value data input to the streamer is specified with the field named "ignite"
(or a different one you configure with StormStreamer.setIgniteTupleField(...)).
See TestStormSpout.declareOutputFields(...) for an example.

4. Create a topology with the streamer and start.
