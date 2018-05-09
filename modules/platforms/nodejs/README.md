# NodeJS Client for Apache Ignite #

This client allows your application to work with the [Apache Ignite platform](https://ignite.apache.org/) via the [Binary Client Protocol](https://apacheignite.readme.io/docs/binary-client-protocol).

The client includes:
- [API specification](https://rawgit.com/nobitlost/ignite/master/modules/platforms/nodejs/api_spec/index.html)
- [implementation](./lib)
- [examples](./examples)
- [tests](./spec)
- docs
  - the main readme (this file)
  - [readme for examples](./examples/README.md)
  - [readme for tests](./spec/README.md)

## Installation ##

[Node.js](https://nodejs.org/en/) version 8 or higher is required. Either download the Node.js [pre-built binary](https://nodejs.org/en/download/) for the target platform, or install Node.js via [package manager](https://nodejs.org/en/download/package-manager).

Once `node` and `npm` are installed, execute the following commands:

(temporary, while the NPM module is not released on [npmjs](https://www.npmjs.com))

1. Clone or download Ignite repository https://github.com/nobitlost/ignite.git to `local_ignite_path`
2. Go to `local_ignite_path/modules/platforms/nodejs` folder
3. Execute `npm link` command
4. Execute `npm link apache-ignite-client` command (needed only for examples and tests)

```bash
cd local_ignite_path/modules/platforms/nodejs
npm link
npm link apache-ignite-client
```

## Supported Features ##

The client supports all operations and types from the [Binary Client Protocol v.2.4](https://apacheignite.readme.io/v2.4/docs/binary-client-protocol) except the following not-applicable features:
- OP_REGISTER_BINARY_TYPE_NAME and OP_GET_BINARY_TYPE_NAME operations are not supported.
- Filter object for OP_QUERY_SCAN operation is not supported. OP_QUERY_SCAN operation itself is supported.
- It is not possible to register a new Ignite Enum type. Reading and writing items of the existing Ignite Enum types are supported.

The following additional features are supported:
- Authentication using username/password.
- SSL/TLS connection.
- "Failover re-connection algorithm".

## API Specification ##

Full specification of the client's public API is available [here](https://rawgit.com/nobitlost/ignite/master/modules/platforms/nodejs/api_spec/index.html)

It is auto-generated from the [jsdoc](http://usejsdoc.org/) comments in source files and located in the [api_spec](./api_spec) folder.

Promises async/await mechanism is used by the API and the client's implementation.

## Data Types ##

The client supports two cases of mapping between Ignite types defined by the Binary Client Protocol and JavaScript types:
- default mapping,
- explicit mapping.

A mapping occurs every time an application writes or reads a field to/from an Ignite cache via the client's API. A field here is any data in a cache - key or value of a cache entry or a map, element of an array or set, field of a complex object, etc.

Using the client's API methods, an application can explicitly specify an Ignite type for a field. The client uses this information during the field read/write operations. It returns the corresponding JavaScript type in results of read operations. It checks the corresponding JavaScript type in inputs of write operations.

If an application does not explicitly specify an Ignite type for a field, the client uses default mapping during the field read/write operations.

Default mapping between Ignite and JavaScript types is described [here](https://rawgit.com/nobitlost/ignite/master/modules/platforms/nodejs/api_spec/ObjectType.html).

### Complex Object Type Support ###

The client provides two ways to operate with the Ignite Complex Object type - in the deserialized form and in the binary form.

An application can specify an Ignite type of a field by an instance of the *ComplexObjectType* class which references an instance of a JavaScript Object. In this case, when the application reads a value of the field, the client deserializes the received Ignite Complex Object and returns it to the client as an instance of the corresponding JavaScript Object. When the application writes a value of the field, the client expects an instance of the corresponding JavaScript Object and serializes it to the Ignite Complex Object.

If an application does not specify an Ignite type of a field and reads a value of the field, the client returns the received Ignite Complex Object as an instance of the *BinaryObject* class - a binary form of the Ignite Complex Object. The *BinaryObject* allows to manipulate with it's content - read and write values of the object's fields, add and remove the fields, etc. Also, an application can create an instance of the *BinaryObject* class from a JavaScript Object. An application can write the *BinaryObject* as a value of a field in a cache, if that field has no explicitly specified Ignite type.

The client takes care of obtaining or registering information about Ignite Complex Object type, including schema, from/at Ignite cluster. It is done automatically by the client, when required for reading or writing of the Ignite Complex Object from/to a cache.

## Usage ##

The below sections exaplains the basic steps to work with Apache Ignite using NodeJS client.

### Instantiate Ignite Client ###

A usage of the client starts from the creation of an *IgniteClient* class instance. The constructor has one, optional, parameter - *onStateChanged* callback which will be called every time the client moves to a new connection state (see below).

It is possible to create as many *IgniteClient* instances as needed. All of them will work fully independently.

TODO - example

### Create Ignite Client Configuration ###

The next step is to define a configuration for the client's connection - create an *IgniteClientConfiguration* class instance.

A mandatory part of the configuration, which is specified in the constructor, is a list of endpoints of the Ignite nodes. At least one endpoint must be specified. A client connects to one node only - a random endpoint from the provided list. Other nodes, if provided, are used by the client for the "failover re-connection algorithm": the client tries to re-connect to the next random endpoint from the list if the current connection has lost.

Optional parts of the configuration can be specified using additional set methods. They include:
- username and password for authentication,
- SSL/TLS connection enabling,
- NodeJS connection options.

By default, the client establishes a non-secure connection with default connection options defined by NodeJS and does not use authentication.

TODO - example

### Connect Ignite Client ###

The next step is to connect the client to an Ignite node. The configuration for the client's connection, which includes endpoint(s) to connect to, is specified in the connect method.

The client has three connection states - *CONNECTING*, *CONNECTED*, *DISCONNECTED*. A state is reported via *onStateChanged* callback, if that was provided in the client's constructor.

Any operations with Ignite caches are possible in the *CONNECTED* state only.

If the client unexpectedly lost the connection, it automatically moves to the *CONNECTING* state and tries to re-connect using the "failover re-connection algorithm". If not possible to connect to all endpoints from the provided list, the client moves to the *DISCONNECTED* state.

At any moment, an application can call the disconnect method and forcibly moves the client to the *DISCONNECTED* state.

When the client becomes disconnected, an application can call the connect method again - with the same or different configuration (eg. with different list of endpoints).

TODO - example

### Obtain Cache Instance ###

The next step is to obtain a Cache instance - an instance of the *CacheClient* class. One Cache instance gives access to one Ignite cache.

The Ignite client provides several methods to manipulate with Ignite caches and obtain a Cache instance - get a cache by it's name, create a cache with the specified name and optional cache configuration, get or create a cache, destroys a cache, etc.

It is possible to obtain as many *CacheClient* instances as needed - for the same or different Ignite caches - and work with all of them "in parallel".

TODO - example

### Configure Cache Instance ###

The next step is optional.

It is possible to specify concrete Ignite types for the key and/or the value of the cache. If the key and/or value is a non-primitive type (eg. a map, a collection, a complex object, etc.) it is possible to specify concrete Ignite types for fields of that objects as well.

If Ignite type is not explicitly specified for some field, the client tries to make automatic default mapping between JavaScript types and Ignite object types.

More details about types and mappings are clarified in the [Data Types](#data-types) section.

TODO - example


Now, everything is ready to manipulate with the data in the cache.

### Key-Value Queries ###

The *CacheClient* class provides methods to manipulate with the key and the value of the cache using Key-Value Queries operations - put, get, put all, get all, replace, clear, etc.

TODO - example

### SQL, SQL Fields and Scan Queries ###

The *CacheClient* class provides the query method that accepts an instance of a concrete query definition class and returns an instance of a concrete cursor class which can be used to obtain the results of the query.

Every cursor class allows
- either to iterate over the results of the query by obtaining one element of the results after another,
- or to get all elements of the results in a one array at once.

#### SQL Query ####

First, define the query by creating and configuring an instance of the *SqlQuery* class.

Then, pass the *SqlQuery* instance in to the query method of the Cache instance and obtain an instance of the *Cursor* class.

Finally, use the *Cursor* instance to iterate over or get all cache entries returned by the query.

TODO - example

#### Scan Query ####

First, define the query by creating and configuring an instance of the *ScanQuery* class.

Then, pass the *ScanQuery* instance in to the query method of the Cache instance and obtain an instance of the *Cursor* class.

Finally, use the *Cursor* instance to iterate over or get all cache entries returned by the query.

TODO - example

#### SQL Fields Query ####

First, define the query by creating and configuring an instance of the *SqlFieldsQuery* class.

Then, pass the *SqlFieldsQuery* instance in to the query method of the Cache instance and obtain an instance of the *SqlFieldsCursor* class.

Finally, use the *SqlFieldsCursor* instance to iterate over or get all elements returned by the query.

TODO - example

### Enable Debug ###

To switch on/off the client's debug output (including errors logging), call *setDebug()* method of the *IgniteClient* instance. Debug output is disabled by default.

