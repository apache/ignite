Apache Ignite AWS Module
------------------------

Apache Ignite AWS module provides S3-based implementations of checkpoint SPI and IP finder for TCP discovery.

Depending on how you use Ignite, you can an extension using one of the following methods:

- If you use the binary distribution, move the libs/{module-dir} to the 'libs' directory of the Ignite distribution before starting the node.
- Add libraries from libs/{module-dir} to the classpath of your application.
- Add a module as a Maven dependency to your project.


Building Module And Running Tests
---------------------------------

To build and run AWS extension use the command below with the right credentials to the AWS service:

mvn clean package -pl modules/aws-ext -Dtest.amazon.access.key=<key> -Dtest.amazon.secret.key=<key>


Importing AWS Module In Maven Project
-------------------------------------

If you are using Maven to manage dependencies of your project, you can add AWS module
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
            <artifactId>ignite-aws-ext</artifactId>
            <version>${ignite-aws-ext.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
