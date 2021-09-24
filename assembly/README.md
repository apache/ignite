# Apache Ignite 3 Alpha 2

Apache Ignite is a distributed database for high-performance computing with in-memory speed.

Ignite 3 is the next generation of the platform that will support a modernized modular architecture,
along with improved usability and developer experience.

The current alpha version includes the following features:
* Unified CLI tool
* New configuration engine
* New schema management engine
* Table API
* Atomic storage implementation based on Raft

## Installation

1. Download Ignite 3 Alpha 2:
   ```
   curl -L "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=ignite/3.0.0-alpha2/apache-ignite-3.0.0-alpha2.zip" -o apache-ignite-3.0.0-alpha2.zip
   ```
2. Unzip the downloaded file:
   ```
   unzip apache-ignite-3.0.0-alpha2.zip && cd apache-ignite-3.0.0-alpha2
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
* `TableExample` - demonstrates the usage of the `org.apache.ignite.table.Table` API
* `KeyValueBinaryViewExample` - demonstrates the usage of the `org.apache.ignite.table.KeyValueView` API

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
