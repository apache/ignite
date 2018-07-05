# Examples #

NodeJS Client for Apache Ignite contains fully workable examples to demonstrate the main behavior of the client.

## Description ##

### Sql Example ###

Source: [SqlExample.js](./SqlExample.js)

This example shows primary APIs to use with Ignite as with an SQL database:
- connects to a node
- creates a cache, if it doesn't exist
- creates tables (CREATE TABLE)
- creates indices (CREATE INDEX)
- writes data of primitive types into the tables (INSERT INTO table)
- reads data from the tables (SELECT ...)
- deletes tables (DROP TABLE)
- destroys the cache

### Cache Put Get Example ###

Source: [CachePutGetExample.js](./CachePutGetExample.js)

This example demonstrates basic Cache, Key-Value Queries and Scan Query operations:
- connects to a node
- creates a cache, if it doesn't exist
  - specifies key type as Integer
- executes different cache operations with Complex Objects and Binary Objects
  - put several objects in parallel
  - putAll
  - get
  - getAll
  - ScanQuery
- destroys the cache

### Sql Query Entries Example ###

Source: [SqlQueryEntriesExample.js](./SqlQueryEntriesExample.js)

This example demonstrates basic Cache, Key-Value Queries and SQL Query operations:
- connects to a node
- creates a cache from CacheConfiguration, if it doesn't exist
- writes data of primitive and Complex Object types into the cache using Key-Value put operation
- reads data from the cache using SQL Query
- destroys the cache

### Auth Tls Example ###

Source: [AuthTlsExample.js](./AuthTlsExample.js)

This example requires [additional setup](#additional-setup-for-authtlsexample).

This example demonstrates how to establish a secure connection to an Ignite node and use username/password authentication, as well as basic Key-Value Queries operations for primitive types:
- connects to a node using TLS and providing username/password
- creates a cache, if it doesn't exist
  - specifies key and value type of the cache
- put data of primitive types into the cache
- get data from the cache
- destroys the cache


### Failover Example ###

Source: [FailoverExample.js](./FailoverExample.js)

This example requires [additional setup](#additional-setup-for-failoverexample).

This example demonstrates the failover behavior of the client
- configures the client to connect to a set of nodes
- connects to a node
- if connection is broken, the client automatically tries to reconnect to another node
- if no specified nodes are available, stops the client


## Installation ##

(temporary, while the NPM module is not released on [npmjs](https://www.npmjs.com))

Examples are installed along with the client.
Follow the [instructions in the main readme](../README.md#installation).

## Setup and Running ##

1. Run Apache Ignite server - locally or remotely.

2. If needed, modify `ENDPOINT` constant in an example source file - Ignite node endpoint. The default value is `127.0.0.1:10800`.

3. Run an example by calling `node <example_file_name>.js`. Eg. `node CachePutGetExample.js`

## Additional Setup for AuthTlsExample ##

1. Obtain certificates required for TLS:
  - either use pre-generated certificates provided in the [examples/certs](./certs) folder. Password for the files: `123456`. Note, these certificates work for an Ignite server installed locally only.
  - or obtain other existing certificates applicable for a concrete Ignite server.
  - or generate new certificates applicable for a concrete Ignite server.

  - The following files are needed:
    - keystore.jks, truststore.jks - for the server side
    - client.key, client.crt, ca.crt - for the client side

2. Place client.key, client.crt and ca.crt files somewhere locally, eg. into the [examples/certs](./certs) folder.

3. If needed, modify `TLS_KEY_FILE_NAME`, `TLS_CERT_FILE_NAME` and `TLS_CA_FILE_NAME` constants in the example source file. The default values point to the files in the [examples/certs](./certs) folder.

4. Setup Apache Ignite server to accept TLS - see appropriate Ignite documentation. Provide the obtained keystore.jks and truststore.jks certificates during the setup.

5. Switch on and setup authentication in Apache Ignite server - see appropriate Ignite documentation.

6. If needed, modify `USER_NAME` and `PASSWORD` constants in the example source file. The default values are the default Ignite username/password.

7. Executes [Setup and Running](#setup-and-running) steps.

## Additional Setup for FailoverExample ##

1. Run three Ignite nodes. See appropriate Ignite documentation for more details.

2. If needed, modify `ENDPOINT1`, `ENDPOINT2`, `ENDPOINT2` constants in an example source file - Ignite node endpoints.
Default values are `localhost:10800`, `localhost:10801`, `localhost:10802` respectively.

2. Run an example by calling `node FailoverExample.js`. 

3. Shut down the node the client connected to (you can find it out from the client logs in the console).

4. From the logs, you will see that the client automatically reconnects to another node which is available.

5. Shut down all the nodes. You will see the client being stopped after failing to connect to each of the nodes.

