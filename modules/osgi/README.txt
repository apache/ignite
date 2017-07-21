Apache Ignite OSGi Integration Module
-------------------------------------

This module provides the bridging components to make Apache Ignite run seamlessly inside an OSGi container
like Apache Karaf. It provides a Bundle Activator to initialize Ignite, along with different classloaders
facilitate class resolution within an OSGi environment.

If using Ignite within Apache Karaf, please refer to the osgi-karaf and osgi-paxlogging modules too:

  - osgi-karaf contains a feature repository to facilitate installing Ignite into a Karaf container.
  - osgi-paxlogging contains an OSGi fragment required to make pax-logging-api expose certain log4j packages
    required by ignite-log4j

Importing the ignite-osgi module in a Maven project
---------------------------------------------------

If you are using Maven to manage dependencies of your project, you can add the ignite-osgi module
dependency like this (replace '${ignite.version}' with actual Ignite version you are interested in):

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-osgi</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
