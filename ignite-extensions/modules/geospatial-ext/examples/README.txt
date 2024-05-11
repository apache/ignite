Apache Ignite Examples
======================

Examples are shipped as a separate Maven project, so to start running you simply need
to import provided `pom.xml` file into your favourite IDE.


Starting Remote Nodes
=====================

Remote nodes for examples should always be started with special configuration file which enables P2P
class loading: `examples/config/example-ignite.xml`. To run a remote node in IDE use `ExampleNodeStartup` class.


LGPL
=====
It is required some lgpl-based libs, for example: ignite-hibernate or ignite-schedule.
In case these libs can not be found by this maven project please download Apache Ignite sources
at https://ignite.apache.org/download.cgi#sources.

There is a way to gain required libs from sources:

Run "mvn clean install -DskipTests " at Apache Ignite sources.
This case will install lgpl-based libs to local maven repository.

Required libs will also appear at /target/release-package/libs/optional sub-folders.
Found libs should be copied to global or project's classpath.
