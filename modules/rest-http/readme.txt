GridGain REST-HTTP Module
-------------------------

GridGain REST-HTTP module provides Jetty-based server which can be used to execute tasks and/or cache commands
in grid using REST approach via HTTP protocol.

To enable REST-HTTP module move 'libs/optional/gridgain-rest-http' folder to 'libs' folder and
add dependency on gridgain-rest-http lib in Maven 'pom.xml' file.

<project xmlns="http://maven.apache.org/POM/4.0.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                      http://maven.apache.org/xsd/maven-4.0.0.xsd">
  ...
  <dependencies>
    ...
    <dependency>
        <groupId>org.gridgain</groupId>
        <artifactId>gridgain-rest-http</artifactId>
        <version>6.1.8</version>
    </dependency>
    ...
  </dependencies>
  ...
</project>
