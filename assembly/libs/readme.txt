To enable any of GridGain module for starting standalone nodes from bin scripts
move corresponding module folder from 'libs/optional' folder to 'libs' folder.
All files and folders contained in 'libs' folder and excluding 'libs/optional'
folder will be added to classpath. It means that all additional project libs
may be placed in 'libs' folder or subfolders excluding 'libs/optional'.

To add any of GridGain module to developed project add dependency on
corresponding lib in Maven 'pom.xml' file.


For example, to add SSH module to developed project add dependency on
gridgain-ssh lib in Maven 'pom.xml' file.

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
