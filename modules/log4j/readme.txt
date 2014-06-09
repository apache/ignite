GridGain Log4J Module
---------------------

GridGain Log4J module provides GridLogger implementation based on Apache Log4J.

To enable Log4J module move 'libs/optional/gridgain-log4j' folder to 'libs' folder and
add dependency on gridgain-log4j lib in Maven 'pom.xml' file.

<project xmlns="http://maven.apache.org/POM/4.0.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                      http://maven.apache.org/xsd/maven-4.0.0.xsd">
  ...
  <dependencies>
    ...
    <dependency>
        <groupId>org.gridgain</groupId>
        <artifactId>gridgain-log4j</artifactId>
        <version>6.1.8</version>
    </dependency>
    ...
  </dependencies>
  ...
</project>
