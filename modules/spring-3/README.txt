Apache Ignite Spring 3 Module
---------------------------

Apache Ignite Spring 3 module provides resources injection capabilities and parser for Spring3-based
configuration XML files. Its purpose is to support compatibility with spring 3.

To enable Spring3 module when starting a standalone node, remove 'libs/ignite-spring/' folder and
 move 'libs/optional/ignite-spring_3' folder to 'libs' folder before running 'ignite.{sh|bat}' script.
The content of the module folder will be added to classpath in this case.

Importing Spring Module In Maven Project
----------------------------------------

If you are using Maven to manage dependencies of your project, you can add Spring module
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
            <artifactId>ignite-spring_3</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
