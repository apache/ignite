Apache Ignite ZooKeeper Ip Finder Module
------------------------------

Apache Ignite ZooKeeper Ip Finder module provides a TCP Discovery IP Finder that uses a ZooKeeper
directory to locate other Ignite nodes to connect to.

Depending on how you use Ignite, you can an extension using one of the following methods:

- If you use the binary distribution, move the libs/{module-dir} to the 'libs' directory of the Ignite distribution before starting the node.
- Add libraries from libs/{module-dir} to the classpath of your application.
- Add a module as a Maven dependency to your project.

The module depends on third-party libraries that use the slf4j facade for logging.
You can set up an underlying logging framework yourself.

Importing Apache Ignite ZooKeeper IpFinder Module In Maven Project
---------------------------------------------------------

If you are using Maven to manage dependencies of your project, you can add the ZooKeeper
module dependency like this (replace '${ignite-zookeeper-ip-finder-ext.version}' with actual Ignite version you
are interested in):

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-zookeeper-ip-finder-ext</artifactId>
            <version>${ignite-zookeeper-ip-finder-ext.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
