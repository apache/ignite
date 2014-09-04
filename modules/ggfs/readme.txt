GridGain Filesystem Module
----------------------

GridGain Filesystem module (GGFS) is high-performance dual-mode in-memory file system. It may be used independently or
as HDFS replacement in Apache Hadoop ecosystem via driver (is placed in gridgain-hadoop module). Also it may work in
some dual modes as wrapper of HDFS or any other Hadoop-compatible file system.

To enable Hadoop module when starting a standalone node, move 'optional/gridgain-ggfs' folder to
'libs' folder before running 'ggstart.{sh|bat}' script. The content of the module folder will
be added to classpath in this case.

Importing Hadoop Module In Maven Project
----------------------------------------

If you are using Maven to manage dependencies of your project, you can add Hadoop module
dependency like this (replace '${gridgain.version}' with actual GridGain version you are
interested in):

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.gridgain</groupId>
            <artifactId>gridgain-filesystem</artifactId>
            <version>${gridgain.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
