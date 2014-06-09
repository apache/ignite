To enable any of GridGain module move corresponds module folder from 'libs/optional'
folder to 'libs' folder and add dependency on lib to Maven 'pom.xml' file.

For example, to enable SSH module move 'libs/optional/gridgain-ssh' folder to
'libs' folder and add dependency on gridgain-ssh lib in Maven 'pom.xml' file.

<project xmlns="http://maven.apache.org/POM/4.0.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                      http://maven.apache.org/xsd/maven-4.0.0.xsd">
  ...
  <dependencies>
    ...
    <dependency>
        <groupId>org.gridgain</groupId>
        <artifactId>gridgain-ssh</artifactId>
        <version>6.1.8</version>
    </dependency>
    ...
  </dependencies>
  ...
</project>
