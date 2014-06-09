GridGain Hibernate Module
-------------------------

GridGain Hibernate module provides Hibernate second-level cache (L2 cache) implementation based
on GridGain In-Memory Data Grid.

To enable Hibernate module move 'libs/optional/gridgain-hibernate' folder to 'libs' folder and
add dependency on gridgain-hibernate lib in Maven 'pom.xml' file.

<project xmlns="http://maven.apache.org/POM/4.0.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                      http://maven.apache.org/xsd/maven-4.0.0.xsd">
  ...
  <dependencies>
    ...
    <dependency>
        <groupId>org.gridgain</groupId>
        <artifactId>gridgain-hibernate</artifactId>
        <version>6.1.8</version>
    </dependency>
    ...
  </dependencies>
  ...
</project>
