Apache Ignite Cloud Module
------------------------

Apache Ignite Cloud module provides Apache jclouds implementations of IP finder for TCP discovery.

To enable Cloud module when starting a standalone node, move 'optional/ignite-cloud' folder to
'libs' folder before running 'ignite.{sh|bat}' script. The content of the module folder will
be added to classpath in this case.

Importing Cloud Module In Maven Project
-------------------------------------

If you are using Maven to manage dependencies of your project, you can add Cloud module
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
            <artifactId>ignite-cloud</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
