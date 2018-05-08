# Test Instructions #

NodeJS Client for Apache Ignite contains [Jasmine](https://www.npmjs.com/package/jasmine) tests to check the behavior of the client.

## Tests Installation ##

(temporary, while the NPM module is not released on [npmjs](https://www.npmjs.com))

Tests are installed along with the client.
Follow the [instructions in the main readme](../README.md#installation).

## Tests Running ##

1. Run Apache Ignite server - locally or remotely.
2. Set the environment variable:
    - **APACHE_IGNITE_CLIENT_ENDPOINTS** - comma separated list of Ignite node endpoints.
    - **APACHE_IGNITE_CLIENT_DEBUG** - (optional) if *true*, tests will display additional output (default: *false*).
3. Alternatively, instead of the environment variables setting, you can directly specify the values of the corresponding variables in [local_ignite_path/modules/platforms/nodejspec/config.js](./config.js) file.
4. Run the tests by calling `npm test` command from `local_ignite_path/modules/platforms/nodejs` folder.
