GridGain Protobuf Module
------------------------

GridGain Protobuf module provides Protobuf support for binary grid client connections. Enable it if you are going
to use C++ and/or .NET clients.

To enable Protobuf module when starting a standalone node, move 'optional/gridgain-protobuf' folder to
'libs' folder before running 'ggstart.{sh|bat}' script. The content of the module folder will
be added to classpath in this case.

Importing Protobuf Module In Maven Project
-------------------------------------

If you are using Maven to manage dependencies of your project, you can add Protobuf module
dependency like this (replace '${gridgain.version}' with actual GridGain version you are
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
            <artifactId>gridgain-protobuf</artifactId>
            <version>${gridgain.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
