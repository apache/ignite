Apache Ignite LGPL Examples
======================

This folder contains code examples for various Apache Ignite functionality.

Examples are shipped as a separate Maven project, so to start running you simply need
to import provided `pom.xml` file into your favourite IDE.

The examples folder contains he following subfolders:

- `config` - contains Ignite configuration files needed for examples.
- `src/main/java` - contains Java examples for different Ignite modules and features.
- `src/main/java8` - contains additional set of Java examples utilizing Java 8 lambdas. These examples
  are excluded by default, enable `java8-examples` Maven profile to include them (JDK8 is required).


Starting Remote Nodes
=====================

Remote nodes for examples should always be started with special configuration file which enables P2P
class loading: `examples/config/example-ignite.xml`. To run a remote node in IDE use `ExampleNodeStartup` class.


Java7 vs Java8
===============
Some examples (not all) which can benefit from Java8 Lambda support were re-written with Java8 lambdas.
For full set of examples, look at both Java7 and Java8 packages.
