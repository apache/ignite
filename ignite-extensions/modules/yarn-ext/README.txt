Apache Ignite Yarn Module
------------------------

Apache Ignite Yarn module provides integration Apache Ignite with Apache Hadoop Yarn.

Importing Apache Ignite Yarn Module In Maven Project
-------------------------------------

If you are using Maven to manage dependencies of your project, you can add Yarn module
dependency like this:

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-yarn-ext</artifactId>
            <version>1.0.0</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
