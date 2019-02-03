Apache Ignite TensorFlow Integration Module
------------------------

Apache Ignite TensorFlow Integration Module allowed using TensorFlow with Apache Ignite. In this scenario Apache Ignite
will be a datasource for any TensorFlow model training.

Import Apache Ignite TensorFlow Integration Module In Maven Project
-------------------------------------

If you are using Maven to manage dependencies of your project, you can add TensorFlow module
dependency like this (replace '${ignite.version}' with actual Ignite version you are
interested in):

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-tensorflow</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
-------------------------------------

TensorFlow integration module provides command line tool that allows to start, maintain and stop distributed deep
learning utilizing Apache Ignite infrastructure and data. This tool provides several commands that are shown here:

Usage: ignite-tf [-hV] [-c=<cfg>] [COMMAND]
Apache Ignite and TensorFlow integration command line utility that allows to
start, maintain and stop distributed deep learning utilizing Apache Ignite
infrastructure and data.
  -c, --config=<cfg>   Apache Ignite client configuration.
  -h, --help           Show this help message and exit.
  -V, --version        Print version information and exit.
Commands:
  start   Starts a new TensorFlow cluster and attaches to user script process.
  stop    Stops a running TensorFlow cluster.
  attach  Attaches to running TensorFlow cluster (user script process).
  ps      Prints identifiers of all running TensorFlow clusters.

To start TensorFlow cluster you need to specify upstream cache that will be used as data source for training, folder
that contains code that actually performs training and command that should be called on this code to start training
correctly. Command "start" have the following help output:

Usage: ignite-tf start [-hV] [-c=<cfg>] CACHE_NAME JOB_DIR JOB_CMD [JOB_ARGS...]
Starts a new TensorFlow cluster and attaches to user script process.
      CACHE_NAME       Upstream cache name.
      JOB_DIR          Job folder (or zip archive).
      JOB_CMD          Job command.
      [JOB_ARGS...]    Job arguments.
  -c, --config=<cfg>   Apache Ignite client configuration.
  -h, --help           Show this help message and exit.
  -V, --version        Print version information and exit.

To attach to running TensorFlow cluster or stop it you can use commands "attach" and "stop" correspondingly. These
commands accepts cluster identifier as a parameter:

Usage: ignite-tf attach [-hV] [-c=<cfg>] CLUSTER_ID
Attaches to running TensorFlow cluster (user script process).
      CLUSTER_ID       Cluster identifier.
  -c, --config=<cfg>   Apache Ignite client configuration.
  -h, --help           Show this help message and exit.
  -V, --version        Print version information and exit.

Usage: ignite-tf stop [-hV] [-c=<cfg>] CLUSTER_ID
Stops a running TensorFlow cluster.
      CLUSTER_ID       Cluster identifier.
  -c, --config=<cfg>   Apache Ignite client configuration.
  -h, --help           Show this help message and exit.
  -V, --version        Print version information and exit.

To find out what TensorFlow clusters are currently running on top of Apache Ignite you can use "ps" command that doesn't
require arguments.