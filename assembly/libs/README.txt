Apache Ignite Dependencies
--------------------------

Current folder contains JAR files for all Apache Ignite modules along with their dependencies.
When node is started using 'ignite.{sh|bat}' script, all JARs and classes located in
'libs' folder and all its sub-folders except 'optional' are added to classpath of the node.

By default, only Apache Ignite core JAR and a minimum set of modules is enabled, while other
modules are located in 'optional' folder and therefore disabled.

To enable any of optional Ignite modules when starting a standalone node,
move corresponding module folder from 'libs/optional' to 'libs' before running
'ignite.{sh|bat}' script. The content of the module folder will be added to
classpath in this case.

If you need to add your own classes to classpath of the node (e.g., task classes), put them
to 'libs' folder. You can create a subfolder for convenience as well.


Importing Ignite Dependencies In Maven Project
------------------------------------------------

If you are using Maven to manage dependencies of your project, there are two options:

1. Import fabric edition:
  - ignite-fabric (all inclusive)

Here is how 'ignite-fabric' can be added to your POM file (replace '${ignite.version}'
with actual Ignite version you are interested in):

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-fabric</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>

2. Or import individual Apache Ignite modules a la carte.

Alternatively you can import Ignite modules a la carte, one by one.
The only required module is 'ignite-core', all others are optional.
Here is how it can be imported into your POM file:

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-core</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>

All optional modules can be imported just like the core module, but with different artifact IDs.

The following modules are available:
- ignite-spring (for Spring-based configuration support)
- ignite-indexing (for SQL querying and indexing)
- ignite-hibernate (for Hibernate integration)
- ignite-web (for Web Sessions Clustering)
- ignite-schedule (for Cron-based task scheduling)
- ignite-logj4 (for Log4j logging)
- ignite-jcl (for Apache Commons logging)
- ignite-jta (for XA integration)
- ignite-hadoop (for Apache Hadoop Accelerator)
- ignite-rest-http (for HTTP REST messages)
- ignite-scalar (for ignite Scala API)
- ignite-sl4j (for SL4J logging)
- ignite-ssh (for starting grid nodes on remote machines)
- ignite-urideploy (for URI-based deployment)
- ignite-aws (for seemless cluster discovery on AWS S3)
- ignite-email (for email alerts)
- ignite-aop (for AOP-based grid-enabling)
- ignite-visor-console (open source command line management and monitoring tool)

For example, if you want to use Apache Ignite Spring-based configuration,
you should add 'ignite-spring' module like this:

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <!-- Core module. -->
        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-core</artifactId>
            <version>${ignite.version}</version>
        </dependency>

        <!-- Optional. -->
        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-spring</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
