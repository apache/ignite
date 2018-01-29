/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const fs = require('fs');
const path = require('path');

require('app-module-path').addPath(path.join(__dirname, 'node_modules'));

const _ = require('lodash');
const getos = require('getos');
const http = require('http');
const https = require('https');
const MigrateMongoose = require('migrate-mongoose');


const packaged = __dirname.startsWith('/snapshot/') || __dirname.startsWith('C:\\snapshot\\');

const igniteModules = !packaged && process.env.IGNITE_MODULES ?
    path.join(path.normalize(process.env.IGNITE_MODULES), 'backend') : path.join(__dirname, 'ignite_modules');

let injector;

try {
    const igniteModulesInjector = path.resolve(path.join(igniteModules, 'injector.js'));

    fs.accessSync(igniteModulesInjector, fs.F_OK);

    process.env.NODE_PATH = path.join(__dirname, 'node_modules');

    injector = require(igniteModulesInjector);
}
catch (ignore) {
    injector = require(path.join(__dirname, 'injector'));
}

/**
 * Event listener for HTTP server "error" event.
 */
const _onError = (addr, error) => {
    if (error.syscall !== 'listen')
        throw error;

    // Handle specific listen errors with friendly messages.
    switch (error.code) {
        case 'EACCES':
            console.error(`Requires elevated privileges for bind to ${addr}`);
            process.exit(1);

            break;
        case 'EADDRINUSE':
            console.error(`${addr} is already in use`);
            process.exit(1);

            break;
        default:
            throw error;
    }
};

/**
 * @param settings
 * @param {ApiServer} apiSrv
 * @param {AgentsHandler} agentsHnd
 * @param {BrowsersHandler} browsersHnd
 */
const init = ([settings, apiSrv, agentsHnd, browsersHnd]) => {
    // Start rest server.
    const srv = settings.server.SSLOptions ? https.createServer(settings.server.SSLOptions) : http.createServer();

    srv.listen(settings.server.port, settings.server.host);

    const addr = `${settings.server.host}:${settings.server.port}`;

    srv.on('error', _onError.bind(null, addr));
    srv.on('listening', () => console.log(`Start listening on ${addr}`));

    apiSrv.attach(srv);

    agentsHnd.attach(srv, browsersHnd);
    browsersHnd.attach(srv, agentsHnd);

    // Used for automated test.
    if (process.send)
        process.send('running');
};

/**
 * Run mongo model migration.
 *
 * @param dbConnectionUri Mongo connection url.
 * @param group Migrations group.
 * @param migrationsPath Migrations path.
 */
const migrate = (dbConnectionUri, group, migrationsPath) => {
    const migrator = new MigrateMongoose({
        migrationsPath,
        dbConnectionUri,
        autosync: true
    });

    console.log(`Running ${group} migrations...`);

    return migrator.run('up')
        .then(() => console.log(`All ${group} migrations finished successfully.`))
        .catch((err) => {
            const msg = _.get(err, 'message');

            if (_.startsWith(msg, 'There are no migrations to run') || _.startsWith(msg, 'There are no pending migrations.')) {
                console.log(`There are no ${group} migrations to run.`);

                return;
            }

            throw err;
        });
};

getos(function(e, os) {
    if (e)
        return console.log(e);

    console.log('Your OS is: ' + JSON.stringify(os));
});

injector.log.info = () => {};
injector.log.debug = () => {};

Promise.all([injector('settings'), injector('mongo')])
    .then(([{mongoUrl}]) => {
        return migrate(mongoUrl, 'Ignite', path.join(__dirname, 'migrations'))
            .then(() => migrate(mongoUrl, 'Ignite Modules', path.join(igniteModules, 'migrations')));
    })
    .then(() => Promise.all([injector('settings'), injector('api-server'), injector('agents-handler'), injector('browsers-handler')]))
    .then(init)
    .catch((err) => {
        console.error(err);

        process.exit(1);
    });
