Apache Ignite Camel Module
--------------------------

Apache Ignite Camel provides a streamer to consume cache tuples from a Camel endpoint such as
HTTP, TCP, File, FTP, AMQP, SNMP, databases, etc. For more information on available components,
refer to http://camel.apache.org/components.html.

To enable the Camel module when starting a standalone node, move 'optional/ignite-camel' folder
to 'libs' folder before running 'ignite.{sh|bat}' script. The content of the module folder will
be added to classpath in this case.

Importing the Camel module in a Maven project
---------------------------------------------

If you are using Maven to manage dependencies of your project, you can add the Camel module
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
            <artifactId>ignite-camel</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
