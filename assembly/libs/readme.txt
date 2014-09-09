GridGain Dependencies
---------------------

Current folder contains JAR files for all GridGain modules along with their dependencies.
When node is started using 'ggstart.{sh|bat}' script, all JARs and classes located in
'libs' folder and all its sub-folders except 'optional' are added to classpath of the node.

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

If you are using Maven to manage dependencies of your project, there are two options:

1. Import gridgain-fabric edition
2. Or import individual GridGain modules a la carte.


Importing GridGain Fabric Dependency
------------------------------------

GridGain Fabric automatically imports GridGain core module and
a set of additional modules needed for this edition to work. Specifically:

- gridgain-core
- gridgain-spring (optional, add if you plan to use Spring configuration)

Here is how 'gridgain-fabric' can be added to your POM file (replace '${gridgain.version}'
with actual GridGain version you are interested in):

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.gridgain</groupId>
            <artifactId>gridgain-fabric</artifactId>
            <version>${gridgain.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>


Importing Individual Modules A La Carte
---------------------------------------

Alternatively you can import GridGain modules a la carte, one by one.
The only required module is 'gridgain-core', all others are optional.
Here is how it can be imported into your POM file:

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

All optional modules can be imported just like the core module, but with different artifact IDs.

The following modules are available:
- gridgain-spring (for Spring-based configuration support)
- gridgain-indexing (for SQL querying and indexing)
- gridgain-hibernate (for Hibernate integration)
- gridgain-web (for Web Sessions Clustering)
- gridgain-schedule (for Cron-based task scheduling)
- gridgain-logj4 (for Log4j logging)
- gridgain-jcl (for Apache Commons logging)
- gridgain-jta (for XA integration)
- gridgain-hadoop (for Apache Hadoop Accelerator)
- gridgain-rest-http (for HTTP REST messages)
- gridgain-scalar (for GridGain Scala API)
- gridgain-sl4j (for SL4J logging)
- gridgain-ssh (for starting grid nodes on remote machines)
- gridgain-urideploy (for URI-based deployment)
- gridgain-aws (for seemless cluster discovery on AWS S3)
- gridgain-email (for email alerts)
- gridgain-aop (for AOP-based grid-enabling)
- gridgain-visor-console (open source command line management and monitoring tool)

For example, if you want to use GridGain Spring-based configuration,
you should add 'gridgain-spring' module like this:

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <!-- Core module. -->
        <dependency>
            <groupId>org.gridgain</groupId>
            <artifactId>gridgain-core</artifactId>
            <version>${gridgain.version}</version>
        </dependency>

        <!-- Optional. -->
        <dependency>
            <groupId>org.gridgain</groupId>
            <artifactId>gridgain-spring</artifactId>
            <version>${gridgain.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
