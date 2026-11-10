Apache Ignite Indexing Module
-----------------------------

Apache Ignite indexing module provides capabilities to index cache context and run SQL, full text or
individual field queries against these indexes.

The module is located in the 'libs' folder of the binary distribution and is enabled by default. It provides
the H2-based SQL engine. When the Calcite-based engine (the 'ignite-calcite' module) is on the classpath too and
`SqlConfiguration.QueryEnginesConfiguration` is not set, the Calcite-based engine is the default one.

Importing indexing Module In Maven Project
------------------------------------------

If you are using Maven to manage dependencies of your project, you can add indexing module
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
            <artifactId>ignite-indexing</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
