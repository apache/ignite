Apache Ignite Web Module
------------------------

Apache Ignite Web module provides Apache Ignite node startups based on servlet and servlet context listener
which allow to start Apache Ignite inside any web container. Additionally this module provides
capabilities to cache web sessions in Apache Ignite cache.

To enable Web module when starting a standalone node, move 'optional/ignite-web' folder to
'libs' folder before running 'ignite.{sh|bat}' script. The content of the module folder will
be added to classpath in this case.

Importing Web Module In Maven Project
-------------------------------------

If you are using Maven to manage dependencies of your project, you can add Web module
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
            <artifactId>ignite-web</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
