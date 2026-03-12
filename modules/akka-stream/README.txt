Apache Ignite Akka Streamer Module
------------------------

Apache Ignite Akka Streamer module provides streaming from Akka Stream to Ignite cache.

Starting data transfer to Ignite cache can be done with the following steps.

1. Import Ignite Akka Streamer Module In Maven Project

If you are using Maven to manage dependencies of your project, you can add Akka-stream module
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
            <artifactId>ignite-akka-stream</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>

2. Create an Ignite configuration file (see example-ignite.xml) and make sure it is accessible from the streamer.

3. Choose what method you will upload data using akka-stream or send directly to Actor.

4. Create your StreamSingleTupleExtractor or StreamMultipleTupleExtractor, an example can be found in the tests
IgniteAkkaActorStreamerSpec#singleExtractor.

5. Create a topology with the streamer and start.