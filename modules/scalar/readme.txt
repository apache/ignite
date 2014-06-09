GridGain Scalar Module
----------------------

GridGain Scalar module provides Scala-based DSL with extensions and shortcuts for GridGain API.

To enable Scalar module move 'libs/optional/gridgain-scalar' folder to 'libs' folder and
add dependency on gridgain-scalar lib in Maven 'pom.xml' file.

<project xmlns="http://maven.apache.org/POM/4.0.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                      http://maven.apache.org/xsd/maven-4.0.0.xsd">
  ...
  <dependencies>
    ...
    <dependency>
        <groupId>org.gridgain</groupId>
        <artifactId>gridgain-scalar</artifactId>
        <version>6.1.8</version>
    </dependency>
    ...
  </dependencies>
  ...
</project>
