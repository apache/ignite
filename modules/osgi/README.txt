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

Running the tests in this module
--------------------------------

We use the Pax Exam framework to fire up an Apache Karaf container (forked process) in order to execute the OSGi tests.

Bundles are provisioned into the container via mvn: URLs. For this to work, you must have run a full build from the
top directory of the Ignite source tree, including the install goal, which provisions the modules into your local
Maven repository:

   mvn clean install -Plgpl

Neither compiling and running the tests, nor generating Javadocs are necessary. To disable these steps,
use these switches:

   -DskipTests -Dmaven.test.skip=true -Dmaven.javadoc.skip=true

You may then run the OSGi test suite:

   mvn test -Dtest=IgniteOsgiTestSuite

NOTE: This test uses environment variables set by the maven-surefire-plugin configuration. If you are running the
test suite from within an IDE, either run it via Maven or set these environment variables manually in your
Run/Debug configuration:

  - projectVersion
  - karafVersion
  - camelVersion

See the pom.xml file of this module to understand which values to set.
