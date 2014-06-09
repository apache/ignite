GridGain Dependencies
---------------------

Current folder contains JAR files for all GridGain modules along with their dependencies.
When node is started using 'ggstart.{sh|bat}' script, all JARs and classes located in
'libs' folder and all its subfolders except 'optional' are added to classpath of the node.

By default, only GridGain core JAR and a minimum set of modules is enabled, while other
modules are located in 'optional' folder and therefore disabled.

To enable any of optional GridGain modules when starting a standalone node,
move corresponding module folder from 'libs/optional' to 'libs' before running
'ggstart.{sh|bat}' script. The content of the module folder will be added to
classpath in this case.

If you need to add your own classes to classpath of the node (e.g., task classes), put them
to 'libs' folder. You can create a subfolder for convenience as well.

Importing GridGain Dependencies In Maven Project
------------------------------------------------

If you are using Maven to manage dependencies of your project, you should add GridGain core
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
            <artifactId>gridgain-core</artifactId>
            <version>${gridgain.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>

All optional modules can be imported using Maven as well. They are added just like the core module,
but with different artifact IDs. E.g., if you want to configure GridGain nodes via Spring, add
'gridgain-spring' module like this:

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.gridgain</groupId>
            <artifactId>gridgain-core</artifactId>
            <version>${gridgain.version}</version>
        </dependency>

        <dependency>
            <groupId>org.gridgain</groupId>
            <artifactId>gridgain-spring</artifactId>
            <version>${gridgain.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
