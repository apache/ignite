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
- ignite-aop (for AOP-based grid-enabling)
- ignite-aws (for seemless cluster discovery on AWS S3)
- ignite-camel (for Apache Camel integration)
- ignite-cassandra (for Apache Cassandra integration)
- ignite-cloud (for Apache JClouds integration) 
- ignite-flink (for streaming from Apache Flink into Ignite)
- ignite-flume (for streaming events from Apache Flume into Ignite)
- ignite-gce (for automatic cluster discovery on Google Compute Engine)
- ignite-hadoop (for Apache Hadoop Accelerator)
- ignite-hibernate (for Hibernate integration)
- ignite-indexing (for SQL querying and indexing)
- ignite-jcl (for Apache Commons logging)
- ignite-jms11 (for streaming messaging from JMS queue or topic into Ignite)
- ignite-jta (for XA integration)
- ignite-kafka (for streaming messages from Apache Kafka into Ignite)
- ignite-logj4 (for Log4j logging)
- ignite-log4j2 (for Log4j 2 logging)
- ignite-mesos (for integration with Apache Mesos cluster resource manager)
- ignite-mqtt (for streaming MQTT topic messages into Ignite)
- ignite-osgi (to allow Ignite run seemlessly in OSGI containers)
- ignite-osgi-karaf (to seemlessly intall ignite into Apache Karaf container)
- ignite-osgi-paxlogging (to expose PAX Logging API to Log4j if needed)
- ignite-rest-http (for HTTP REST messages)
- ignite-scalar (for ignite Scala API)
- ignite-scalar_2.10 (for Ignite Scala 2.10 API)
- ignite-schedule (for Cron-based task scheduling)
- ignite-sl4j (for SL4J logging)
- ignite-spark (for shared in-memory RDDs and faster SQL for Apache Spark)
- ignite-spark_2.10 (for shared in-memory RDDs and faster SQL for Apache Spark with Scala 2.10)
- ignite-spring (for Spring-based configuration support)
- ignite-ssh (for starting grid nodes on remote machines)
- ignite-storm (for streaming events from Apache Storm into Ignite)
- ignite-twitter (for streaming tweets from Twitter into Ignite)
- ignite-urideploy (for URI-based deployment)
- ignite-web (for Web Sessions Clustering)
- ignite-yarn (for integration with Apache Hadoop Yarn)
- ignite-zookeeper (for cluster discovery based on Apache Zookeeper)

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
