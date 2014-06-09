GridGain Indexing Module
------------------------

GridGain indexing module provides capabilities to index cache context and run SQL, full text or
individual field queries against these indexes.

To enable indexing module move 'libs/optional/gridgain-indexing' folder to 'libs' folder and
add dependency on gridgain-indexing lib in Maven 'pom.xml' file.

<project xmlns="http://maven.apache.org/POM/4.0.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                      http://maven.apache.org/xsd/maven-4.0.0.xsd">
  ...
  <dependencies>
    ...
    <dependency>
        <groupId>org.gridgain</groupId>
        <artifactId>gridgain-indexing</artifactId>
        <version>6.1.8</version>
    </dependency>
    ...
  </dependencies>
  ...
</project>
