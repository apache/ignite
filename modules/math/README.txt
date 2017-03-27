Apache Ignite Math Module
------------------------------

Apache Ignite Math module provides several implementations of vector and matrices.

Module includes on heap and off heap, dense and sparse, local and distributed implementations.

Based on ideas from Apache Mahout.

Importing Math Module In Maven Project
---------------------------------------

If you are using Maven to manage dependencies of your project, you can add Log4J2 module
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
            <artifactId>ignite-math</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>