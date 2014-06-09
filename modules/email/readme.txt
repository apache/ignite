GridGain Email Module
---------------------

GridGain email module enables GridGain to send emails in critical situations such as license
expiration or fatal system errors (this should be also configured via 'GridConfiguration.setSmtpXXX(..)'
configuration properties).


To enable email module move 'libs/optional/gridgain-email' folder to 'libs' folder and
add dependency on gridgain-email lib in Maven 'pom.xml' file.

<project xmlns="http://maven.apache.org/POM/4.0.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                      http://maven.apache.org/xsd/maven-4.0.0.xsd">
  ...
  <dependencies>
    ...
    <dependency>
        <groupId>org.gridgain</groupId>
        <artifactId>gridgain-email</artifactId>
        <version>6.1.8</version>
    </dependency>
    ...
  </dependencies>
  ...
</project>
