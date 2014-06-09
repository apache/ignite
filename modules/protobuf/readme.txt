GridGain Protobuf Module
------------------------

GridGain Protobuf module provides Protobuf support for binary REST server. Enable it if you are going
to use C++ and/or .NET clients.

To enable Protobuf module move 'libs/optional/gridgain-protobuf' folder to 'libs' folder and
add dependency on gridgain-protobuf lib in Maven 'pom.xml' file.

<project xmlns="http://maven.apache.org/POM/4.0.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                      http://maven.apache.org/xsd/maven-4.0.0.xsd">
  ...
  <dependencies>
    ...
    <dependency>
        <groupId>org.gridgain</groupId>
        <artifactId>gridgain-protobuf</artifactId>
        <version>6.1.8</version>
    </dependency>
    ...
  </dependencies>
  ...
</project>
