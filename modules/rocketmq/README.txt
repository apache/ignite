Apache Ignite RocketMQ Streamer Module
--------------------------------------

Apache Ignite RocketMQ Streamer module provides streaming from RocketMQ to Ignite cache.

To use Ignite RocketMQ Streamer module, first import it to your Maven project.

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-rocketmq</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>

Then, initialize and start it as, for instance, done in RocketMQStreamerTest.java.
