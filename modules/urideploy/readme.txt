GridGain URI Deploy Module
--------------------------

GridGain URI Deploy module provides capabilities to deploy tasks from different sources like
file system folders, FTP, email and HTTP.

To enable URI Deploy module move 'libs/optional/gridgain-urideploy' folder to 'libs' folder and
add dependency on gridgain-urideploy lib in Maven 'pom.xml' file.

<project xmlns="http://maven.apache.org/POM/4.0.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                      http://maven.apache.org/xsd/maven-4.0.0.xsd">
  ...
  <dependencies>
    ...
    <dependency>
        <groupId>org.gridgain</groupId>
        <artifactId>gridgain-urideploy</artifactId>
        <version>6.1.8</version>
    </dependency>
    ...
  </dependencies>
  ...
</project>
