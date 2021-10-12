# Apache Ignite 3 Alpha 3

Apache Ignite is a distributed database for high-performance computing with in-memory speed.

Ignite 3 is the next generation of the platform that will support a modernized modular architecture,
along with improved usability and developer experience.

The current alpha version includes the following features:
* Unified CLI tool
* New configuration engine
* New schema management engine
* Table API
* Atomic storage implementation based on Raft
* New SQL engine based on Apache Calcite and JDBC driver
* New binary client protocol and its implementation in Java

## Installation

1. Download Ignite 3 Alpha 3:
   ```
   curl -L "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=ignite/3.0.0-alpha3/apache-ignite-3.0.0-alpha3.zip" -o apache-ignite-3.0.0-alpha3.zip
   ```
2. Unzip the downloaded file:
   ```
   unzip apache-ignite-3.0.0-alpha3.zip && cd apache-ignite-3.0.0-alpha3
   ```
3. Add your installation directory to the PATH environment variable:
   ```
   echo 'export IGNITE_HOME="'`pwd`'"' >> ~/.bash_profile && echo 'export PATH="$IGNITE_HOME:$PATH"' >> ~/.bash_profile && source ~/.bash_profile
   ```
4. (optional) If you will start the cluster locally, install the core artifacts:
   ```
   ignite init
   ```

## Running Examples

Examples are shipped as a separate Maven project, which is located in the `examples` folder.
To start running you simply need to import provided `pom.xml` file into your favourite IDE.

The following examples are included:
* `RecordViewExample` - demonstrates the usage of the `org.apache.ignite.table.RecordView` API
* `KeyValueViewExample` - demonstrates the usage of the `org.apache.ignite.table.KeyValueView` API
* `SqlJdbcExample` - demonstrates the usage of the Apache Ignite JDBC driver.
* `RebalanceExample` - demonstrates the data rebalancing process.

To run the `RebalanceExample`, refer to its JavaDoc for instructions.

To run any other example, do the following:
1. Import the examples project into you IDE.
2. Start a server node using the CLI tool:
   ```
   ignite node start --config=$IGNITE_HOME/examples/config/ignite-config.json my-first-node
   ```
3. Run the preferred example in the IDE.
