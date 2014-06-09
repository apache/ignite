GridGain Web Module
-------------------

GridGain Web module provides GridGain node startups based on servlet and servlet context listener
which allow to start GridGain inside any web container. Additionally this module provides
capabilities to cache web sessions in GridGain cache.

To enable Web module for starting standalone nodes from bin scripts move
'libs/optional/gridgain-web' folder to 'libs' folder.

To add Web module to developed project add dependency on gridgain-web lib
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
            <artifactId>gridgain-web</artifactId>
            <version>${gridgain.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
