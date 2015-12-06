Apache Ignite Cassandra Module
------------------------

Apache Ignite Cassandra module provides CacheStore implementation backed by Cassandra database.

To enable Cassandra module when starting a standalone node, move 'optional/ignite-cassandra' folder to
'libs' folder before running 'ignite.{sh|bat}' script. The content of the module folder will
be added to classpath in this case.

Importing Cassandra Module In Maven Project
-------------------------------------

If you are using Maven to manage dependencies of your project, you can add Cassandra module
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
            <artifactId>ignite-cassandra</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
