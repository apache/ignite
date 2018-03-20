# NodeJS Client for Apache Ignite #

This client allows your application to work with the [Apache Ignite platform](https://ignite.apache.org/) via the [Binary Client Protocol](https://apacheignite.readme.io/docs/binary-client-protocol).

This version of the client supports the following functionality:

- [Binary Client Protocol v.2.4](https://apacheignite.readme.io/v2.4/docs/binary-client-protocol)
- subset of [standard data types](https://apacheignite.readme.io/v2.4/docs/binary-client-protocol#section-data-objects) (complex objects are not supported)
- the client connection and disconnection ("failover" reconnection algorithm is not supported)
- some of the [Cache Configuration operations](https://apacheignite.readme.io/v2.4/docs/binary-client-protocol-cache-configuration-operations) to get or create cache, destroy cache and get cache names
- few [Key-Value Queries operations](https://apacheignite.readme.io/v2.4/docs/binary-client-protocol-key-value-operations) to get and put value

This version includes:
- the client [API classes](./lib)
- an [example](./examples)
- [Jasmine](https://www.npmjs.com/package/jasmine) [tests](./spec)

## Installation ##

[Node.js](https://nodejs.org/en/) version 8 or higher is required. You can download the Node.js [pre-built binary](https://nodejs.org/en/download/) for your platform, or install Node.js via [package manager](https://nodejs.org/en/download/package-manager). Once `node` and `npm` are installed, you need to execute the following command:

(temporary, while the NPM module is not released on [npmjs](https://www.npmjs.com))

1. Clone or download Ignite repository https://github.com/nobitlost/ignite.git to `local_ignite_path`
2. Go to `local_ignite_path/modules/clients/nodejs` folder
3. Execute `npm link` command
4. Execute `npm install` command (needed only if you are going to run tests)
5. Go to `local_ignite_path/modules/clients/nodejs/examples` folder
6. Execute `npm install apache-ignite-client` command (needed only if you are going to run example)

```bash
cd local_ignite_path/modules/clients/nodejs
npm link
npm install
cd examples
npm install apache-ignite-client
```

## Example Running ##

1. Run Apache Ignite server - locally or remotely.
2. If needed, modify `ENDPOINT` constant in the [example code](./examples/CachePutGetExample.js) - Ignite node endpoint. The default value is `127.0.0.1:10800`.
3. Run the example by calling `node CachePutGetExample.js`

## Tests Running ##

1. Run Apache Ignite server - locally or remotely.
2. Set the environment variable:
    - **APACHE_IGNITE_CLIENT_ENDPOINTS** - comma separated list of Ignite node endpoints (this version uses the first one only).
    - **APACHE_IGNITE_CLIENT_DEBUG** - (optional) if *true*, tests will display additional output (default: *false*).
3. Alternatively, instead of the environment variables setting, you can directly specify the values of the corresponding variables in your local [spec/config.js file](./spec/config.js).
4. Run the tests by calling `npm test` command from your local `local_ignite_path/modules/clients/nodejs` folder.
