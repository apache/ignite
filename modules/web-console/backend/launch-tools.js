/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

'use strict';

const _ = require('lodash');
const http = require('http');
const https = require('https');
const MigrateMongoose = require('migrate-mongoose');
const mongoose = require('mongoose');

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
    const sslOptions = settings.server.SSLOptions;

    console.log(`Starting ${sslOptions ? 'HTTPS' : 'HTTP'} server`);

    const srv = sslOptions ? https.createServer(sslOptions) : http.createServer();

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
 * @param collectionName Name of collection where migrations write info about applied scripts.
 */
const migrate = (dbConnectionUri, group, migrationsPath, collectionName) => {
    const migrator = new MigrateMongoose({
        migrationsPath,
        dbConnectionUri,
        collectionName,
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

/**
 * Check version of used MongoDB.
 */
const checkMongo = () => {
    const versionValid = (mijor, minor) => mijor === 3 && minor >= 2 && minor <= 4;

    const admin = new mongoose.mongo.Admin(mongoose.connection.db, null, global.Promise);

    return admin.buildInfo()
        .then((info) => {
            const versions = info.version.split('.');

            if (!versionValid(parseInt(versions[0]), parseInt(versions[1])))
                throw Error(`Unsupported version of MongoDB ${info.version}. Supported versions: 3.2.x-3.4.x`);
        });
};

module.exports = { checkMongo, migrate, init };
