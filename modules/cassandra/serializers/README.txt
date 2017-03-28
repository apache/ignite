Apache Ignite Cassandra Serializers Module
------------------------

Apache Ignite Cassandra Serializers module provides additional serializers to store objects as BLOBs in Cassandra. The
module could be used as an addition to Ignite Cassandra Store module.

To enable Cassandra Serializers module when starting a standalone node, move 'optional/ignite-cassandra-serializers'
folder to 'libs' folder before running 'ignite.{sh|bat}' script. The content of the module folder will be added to
classpath in this case. Note, copying folder 'optional/ignite-cassandra-serializers' requires copying
'optional/ignite-cassandra-store' folder.

Importing Cassandra Serializers Module In Maven Project
-------------------------------------

If you are using Maven to manage dependencies of your project, you can add Cassandra Store module
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
            <artifactId>ignite-cassandra-serializers</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
