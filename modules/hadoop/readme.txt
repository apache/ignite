GridGain Hadoop Module
----------------------

GridGain Hadoop module provides In-Memory Accelerator For Hadoop functionality which is based
on GGFS - high-performance dual-mode in-memory file system that is 100% compatible with HDFS.

To enable Hadoop module for starting standalone nodes from bin scripts move
'libs/optional/gridgain-hadoop' folder to 'libs' folder.

To add Hadoop module to developed project add dependency on gridgain-hadoop lib
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
            <artifactId>gridgain-hadoop</artifactId>
            <version>${gridgain.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
