Apache Ignite Hadoop Module
---------------------------

Apache Ignite Hadoop module provides In-Memory MapReduce engine and driver to use IGFS as Hadoop file system
which are 100% compatible with HDFS and YARN.

To enable Hadoop module when starting a standalone node, move 'optional/ignite-hadoop' folder to
'libs' folder before running 'ignite.{sh|bat}' script. The content of the module folder will
be added to classpath in this case.

Importing Hadoop Module In Maven Project
----------------------------------------

If you are using Maven to manage dependencies of your project, you can add Hadoop module
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
            <artifactId>ignite-hadoop</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
