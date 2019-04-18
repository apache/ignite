GridGain Storm Streamer Module
-----------------------------------

GridGain Storm Streamer module provides streaming via Storm to GridGain cache.

Starting data transfer to GridGain cache can be done with the following steps.

1. Import GridGain Storm Streamer Module In Maven Project

If you are using Maven to manage dependencies of your project, you can add Storm module
dependency like this (replace '${ignite.version}' with actual GridGain version you are
interested in):

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.gridgain</groupId>
            <artifactId>ignite-storm</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>

2. Create an GridGain configuration file (see example-ignite.xml) and make sure it is accessible from the streamer.

3. Make sure your key-value data input to the streamer is specified with the field named "ignite"
(or a different one you configure with StormStreamer.setIgniteTupleField(...)).
See TestStormSpout.declareOutputFields(...) for an example.

4. Create a topology with the streamer and start.
