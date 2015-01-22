GridGain Web Module
-------------------

GridGain Web module provides GridGain node startups based on servlet and servlet context listener
which allow to start GridGain inside any web container. Additionally this module provides
capabilities to cache web sessions in GridGain cache.

To enable Web module when starting a standalone node, move 'optional/gridgain-web' folder to
'libs' folder before running 'ggstart.{sh|bat}' script. The content of the module folder will
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
            <groupId>org.gridgain</groupId>
            <artifactId>gridgain-web</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
