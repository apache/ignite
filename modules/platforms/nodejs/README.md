# NodeJS Client for Apache Ignite #

## Installation ##

[Node.js](https://nodejs.org/en/) version 8 or higher is required. Either download the Node.js [pre-built binary](https://nodejs.org/en/download/) for the target platform, or install Node.js via [package manager](https://nodejs.org/en/download/package-manager).

Once `node` and `npm` are installed, you can use one of the following installation options.

### Installation via npm ###

Execute the following command to install the Node.js Thin Client package:

```
npm install -g apache-ignite-client
```

### Installation from Sources ###

If you want to install the Thin Client library from Ignite sources, please follow the steps:

1. Download Ignite sources to `local_ignite_path`
2. Go to `local_ignite_path/modules/platforms/nodejs` folder
3. Execute `npm link` command
4. Execute `npm link apache-ignite-client` command (needed only for examples)

```bash
cd local_ignite_path/modules/platforms/nodejs
npm link
npm link apache-ignite-client #linking examples (optional)
```

For more information, see [Apache Ignite Node.JS Thin Client documentation](https://apacheignite.readme.io/docs/nodejs-thin-client).
