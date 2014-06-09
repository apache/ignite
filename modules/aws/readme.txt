GridGain AWS Module
-------------------

GridGain AOP module provides S3-based implementations of checkpoint SPI, IP finder and
metrics store for TCP discovery.

To enable AWS module move 'libs/optional/gridgain-aws' folder to 'libs' folder and
add dependency on gridgain-aws lib in Maven 'pom.xml' file.

<project xmlns="http://maven.apache.org/POM/4.0.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                      http://maven.apache.org/xsd/maven-4.0.0.xsd">
  ...
  <dependencies>
    ...
    <dependency>
        <groupId>org.gridgain</groupId>
        <artifactId>gridgain-aws</artifactId>
        <version>6.1.8</version>
    </dependency>
    ...
  </dependencies>
  ...
</project>
