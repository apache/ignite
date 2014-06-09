GridGain Spring Module
----------------------

GridGain Spring module provides resources injection capabilities and parser for Spring-based
configuration XML files.

To enable Spring module move 'libs/optional/gridgain-spring' folder to 'libs' folder and
add dependency on gridgain-spring lib in Maven 'pom.xml' file.

<project xmlns="http://maven.apache.org/POM/4.0.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                      http://maven.apache.org/xsd/maven-4.0.0.xsd">
  ...
  <dependencies>
    ...
    <dependency>
        <groupId>org.gridgain</groupId>
        <artifactId>gridgain-spring</artifactId>
        <version>6.1.8</version>
    </dependency>
    ...
  </dependencies>
  ...
</project>
