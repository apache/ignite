GridGain SSH Module
-------------------

GridGain SSH module provides capabilities to start GridGain nodes on remote machines via SSH.

To enable SSH module for starting standalone nodes from bin scripts move
'libs/optional/gridgain-ssh' folder to 'libs' folder.

To add SSH module to developed project add dependency on gridgain-ssh lib
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
            <artifactId>gridgain-ssh</artifactId>
            <version>${gridgain.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
