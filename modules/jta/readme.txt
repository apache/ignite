GridGain JTA Module
-------------------

GridGain JTA module provides capabilities to integrate GridGain cache transactions with JTA.

To enable JTA module for starting standalone nodes from bin scripts move
'libs/optional/gridgain-jta' folder to 'libs' folder.

To add JTA module to developed project add dependency on gridgain-jta lib
in Maven 'pom.xml' file.

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.gridgain</groupId>
            <artifactId>gridgain-jta</artifactId>
            <version>${gridgain.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
