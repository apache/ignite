Apache Ignite SSH Module
------------------------

Apache Ignite SSH module provides capabilities to start Apache Ignite nodes on remote machines via SSH.

To enable SSH module when starting a standalone node, move 'optional/ignite-ssh' folder to
'libs' folder before running 'ignite.{sh|bat}' script. The content of the module folder will
be added to classpath in this case.

Importing SSH Module In Maven Project
-------------------------------------

If you are using Maven to manage dependencies of your project, you can add SSH module
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
            <artifactId>ignite-ssh</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
