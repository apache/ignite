GridGain MLeap Parser Module
----------------------------

GridGain MLeap Parser module provides a model and pipeline importer from
Apache Spark using MLeap runtime for further inference.


Importing GridGain MLeap Parser Module In Maven Project
-------------------------------------------------------

If you are using Maven to manage dependencies of your project, you can add the MLeap Parser module
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
            <artifactId>ignite-ml-mleap-model-parser</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>