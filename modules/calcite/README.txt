Apache Ignite Calcite Module
--------------------------

Apache Ignite Calcite module provides experimental Apache Calcite based query engine.

To enable Calcite module when starting a standalone node, move 'optional/ignite-calcite' folder to
'libs' folder before running 'ignite.{sh|bat}' script. The content of the module folder will
be added to classpath in this case.

Note: At now some logic from ignite-indexing module is reused, this means ignite-indexing module also
has to be present at classpath.

Importing Calcite Module In Maven Project
---------------------------------------

If you are using Maven to manage dependencies of your project, you can add Calcite module
dependency like this (replace '${ignite.version}' with actual Apache Ignite version you are
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
            <artifactId>ignite-calcite</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
