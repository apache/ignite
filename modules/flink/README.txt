GridGain Flink Sink Module
-----------------------------------

GridGain Flink Sink module is a streaming connector to inject Flink data into GridGain cache.

Starting data transfer to GridGain can be done with the following steps.

1. Import GridGain Flink Sink Module in Maven Project

If you are using Maven to manage dependencies of your project, you can add Flink module
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
            <artifactId>ignite-flink</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>

2. Create an GridGain configuration file (see example-ignite.xml) and make sure it is accessible from the sink.

3. Make sure your data input to the sink is specified. For example `input.addSink(igniteSinkObject)`
