# Tests #

NodeJS Client for Apache Ignite contains [Jasmine](https://www.npmjs.com/package/jasmine) tests to check the behavior of the client. the tests include:
- functional tests which cover all API methods of the client
- examples executors which run all examples except AuthTlsExample
- AuthTlsExample executor

## Tests Installation ##

(temporary, while the NPM module is not released on [npmjs](https://www.npmjs.com))

Tests are installed along with the client.
Follow the [instructions in the main readme](../README.md#installation).

## Tests Running ##

1. Run Apache Ignite server locally or remotely with default configuration.
2. Set the environment variable:
    - **APACHE_IGNITE_CLIENT_ENDPOINTS** - comma separated list of Ignite node endpoints.
    - **APACHE_IGNITE_CLIENT_DEBUG** - (optional) if *true*, tests will display additional output (default: *false*).
3. Alternatively, instead of the environment variables setting, you can directly specify the values of the corresponding variables in [local_ignite_path/modules/platforms/nodejspec/config.js](./config.js) file.
4. Run the tests:

### Run Functional Tests ###

Call `npm test` command from `local_ignite_path/modules/platforms/nodejs` folder.

### Run Examples Executors ###

Call `npm run test:examples` command from `local_ignite_path/modules/platforms/nodejs` folder.

### Run AuthTlsExample Executor ###

It requires running Apache Ignite server with non-default configuration (authentication and TLS switched on).

If the server runs locally:
- setup the server to accept TLS. During the setup use `keystore.jks` and `truststore.jks` certificates from `local_ignite_path/modules/platforms/nodejs/examples/certs/` folder. Password for the files: `123456`
- switch on the authentication on the server. Use the default username/password.

If the server runs remotely, and/or other certificates are required, and/or non-default username/password is required - see this [instruction](../examples/README.md#additional-setup-for-authtlsexample).

Call `npm run test:auth_example` command from `local_ignite_path/modules/platforms/nodejs` folder.
