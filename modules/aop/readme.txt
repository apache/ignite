GridGain AOP Module
-------------------

GridGain AOP module provides capability to turn any Java method to a distributed closure by
adding @Gridify annotation to it.

To enable AOP module for starting standalone nodes from bin scripts move
'libs/optional/gridgain-aop' folder to 'libs' folder.

To add AOP module to developed project add dependency on gridgain-aop lib
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
            <artifactId>gridgain-aop</artifactId>
            <version>${gridgain.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
