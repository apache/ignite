Apache Ignite Examples
----------------------

This folder contains code examples for various Apache Ignite functionality.

Examples are shipped as a separate Maven project, so to start running you simply need
to import provided `pom.xml` file into your favourite IDE.

The examples folder contains he following subfolders:

- `config` - contains Ignite configuration files needed for examples.
- `memcached` - contains PHP script demonstrating how Ignite Cache can be accessed using Memcached client.
- `rest` - contains PHP script demonstrating how Ignite Cache can be accessed via HTTP API.
- `schema-import` - contains demo project for Apache Ignite Schema Import Utility. Refer to enclosed
  README.txt file for more information on how to build and run the demo.
- `src/main/java` - contains Java examples for different Ignite modules and features.
- `src/main/java8` - contains additional set of Java examples utilizing Java 8 lambdas. These examples
  are excluded by default, enable `java8-examples` Maven profile to include them (JDK8 is required).
- `src/main/scala` - contains examples demonstrating usage of API provided by Scalar.

Starting Remote Nodes
---------------------

Remote nodes for examples should always be started with special configuration file which enables P2P
class loading: `examples/config/example-ignite.xml`. To run a remote node in IDE use `ExampleNodeStartup` class.
