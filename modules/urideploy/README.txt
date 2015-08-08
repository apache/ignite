Apache Ignite URI Deploy Module
-------------------------------

Apache Ignite URI Deploy module provides capabilities to deploy tasks from different sources like
File System, HTTP, or even Email.

To enable URI Deploy module when starting a standalone node, move 'optional/ignite-urideploy' folder to
'libs' folder before running 'ignite.{sh|bat}' script. The content of the module folder will
be added to classpath in this case.

Importing URI Deploy Module In Maven Project
--------------------------------------------

If you are using Maven to manage dependencies of your project, you can add URI Deploy module
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
            <artifactId>ignite-urideploy</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
