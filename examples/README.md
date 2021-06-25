# Apache Ignite 3 Examples

This project contains code examples for Apache Ignite 3.

Examples are shipped as a separate Maven project, so to start running you simply need
to import provided `pom.xml` file into your favourite IDE.

The following examples are included:
* `TableExample` - demonstrates the usage of the `org.apache.ignite.table.Table` API
* `KeyValueBinaryViewExample` - demonstrates the usage of the `org.apache.ignite.table.KeyValueBinaryView` API

To run an example, do the following:
1. Import the examples project into you IDE.
2. (optional) Run one or more standalone nodes using the CLI tool:
    ```
    ignite node start --config=$IGNITE_HOME/examples/config/ignite-config.json node-1
    ignite node start --config=$IGNITE_HOME/examples/config/ignite-config.json node-2
    ...
    ignite node start --config=$IGNITE_HOME/examples/config/ignite-config.json node-n
    ```
3. Run the preferred example in the IDE.
