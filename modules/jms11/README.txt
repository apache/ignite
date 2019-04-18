GridGain JMS 1.1 Module
----------------------------

GridGain JMS 1.1 module provides a streamer to consume JMS queue and topic messages into
GridGain caches.

Importing GridGain JMS 1.1 Module In Maven Project
--------------------------------------------------------

If you are using Maven to manage dependencies of your project, you can add the JMS 1.1 module
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
            <artifactId>ignite-jms11</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
