Apache Ignite Examples
======================

This folder contains code examples for various Apache Ignite functionality.

Examples are shipped as a separate Maven project, so to start running you simply need
to import provided `pom.xml` file into your favourite IDE.

The examples folder contains he following subfolders:

- `config` - contains Ignite configuration files needed for examples.
- `memcached` - contains PHP script demonstrating how Ignite Cache can be accessed using Memcached client.
- `rest` - contains PHP script demonstrating how Ignite Cache can be accessed via HTTP API.
- `sql` - contains sample SQL scripts and data sets.
- `src/main/java` - contains Java examples for different Ignite modules and features.
- `src/main/scala` - contains examples demonstrating usage of API provided by Scalar.
- `src/main/java-lgpl` - contains lgpl-based examples for different Ignite modules and features.

Starting Remote Nodes
=====================

Remote nodes for examples should always be started with special configuration file which enables P2P
class loading: `examples/config/example-ignite.xml`. To run a remote node in IDE use `ExampleNodeStartup` class.


LGPL
=====
LGPL examples can be activated by turning lgpl profile on.

lgpl profile required some lgpl-based libs, for example: ignite-hibernate & ignite-schedule.
In case these libs can not be found by this maven project please download Apache Ignite sources
at https://ignite.apache.org/download.cgi#sources.

There are some ways to gain required libs from sources:

1) Run "mvn clean install -DskipTests -P lgpl" at Apache Ignite sources.
This case will install lgpl-based libs to local maven repository.

2) Run "mvn clean package -DskipTests -Prelease,lgpl -Dignite.edition=apache-ignite-lgpl" at Apache Ignite sources.
Required libs will appear at /target/release-package/libs/optional subfolders.
Found libs should be copied to global or project's classpath.
