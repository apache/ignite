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

