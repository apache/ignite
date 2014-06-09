GridGain JCL Module
-------------------

GridGain JCL module provides GridLogger implementation that can delegate to any logger based
on Jakarta Commons Logging (JCL).

To enable JCL module move 'libs/optional/gridgain-jcl' folder to 'libs' folder and
add dependency on gridgain-jcl lib in Maven 'pom.xml' file.

<project xmlns="http://maven.apache.org/POM/4.0.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                      http://maven.apache.org/xsd/maven-4.0.0.xsd">
  ...
  <dependencies>
    ...
    <dependency>
        <groupId>org.gridgain</groupId>
        <artifactId>gridgain-jcl</artifactId>
        <version>6.1.8</version>
    </dependency>
    ...
  </dependencies>
  ...
</project>
