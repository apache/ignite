Apache Ignite H2 Module
-----------------------------

Apache Ignite H2 module provides SQL engine for ignite indexing.

To enable H2 module when starting a standalone node, move 'optional/ignite-h2' folder to
'libs' folder before running 'ignite.{sh|bat}' script. The content of the module folder will
be added to classpath in this case.

Importing h2 Module In Maven Project
------------------------------------------

If you are using Maven to manage dependencies of your project, you can add H2 module
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
            <artifactId>ignite-h2</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
