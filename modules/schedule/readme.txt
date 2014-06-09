GridGain Schedule Module
------------------------

GridGain Schedule module provides functionality for scheduling jobs locally using UNIX cron-based syntax.

To enable Schedule module for starting standalone nodes from bin scripts move
'libs/optional/gridgain-schedule' folder to 'libs' folder.

To add Schedule module to developed project add dependency on gridgain-schedule lib
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
            <artifactId>gridgain-schedule</artifactId>
            <version>${gridgain.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
