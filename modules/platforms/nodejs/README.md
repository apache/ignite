# NodeJS Client for GridGain #

## Installation ##

[Node.js](https://nodejs.org/en/) version 8 or higher is required. Either download the Node.js [pre-built binary](https://nodejs.org/en/download/) for the target platform, or install Node.js via [package manager](https://nodejs.org/en/download/package-manager).

Once `node` and `npm` are installed, you can use one of the following installation options.

### Installation via npm ###

Execute the following command to install the Node.js Thin Client package:

```
npm install -g gridgain-client
```

### Installation from Sources ###

If you want to install the Thin Client library from GridGain sources, please follow the steps:

1. Download GridGain sources to `local_gridgain_path`
2. Go to `local_gridgain_path/modules/platforms/nodejs` folder
3. Execute `npm link` command
4. Execute `npm link gridgain-client` command (needed only for examples)

```bash
cd local_gridgain_path/modules/platforms/nodejs
npm link
npm link gridgain-client #linking examples (optional)
```

For more information, see [GridGain Node.JS Thin Client documentation](https://apacheignite.readme.io/docs/nodejs-thin-client).