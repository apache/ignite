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
const http = require('http');
const https = require('https');

const igniteModules = process.env.IGNITE_MODULES || './ignite_modules';

let injector;

try {
    const igniteModulesInjector = path.resolve(path.join(igniteModules, 'backend', 'injector.js'));

    fs.accessSync(igniteModulesInjector, fs.F_OK);

    injector = require(igniteModulesInjector);
}
catch (ignore) {
    injector = require(path.join(__dirname, './injector'));
}

/**
 * Event listener for HTTP server "error" event.
 */
const _onError = (port, error) => {
    if (error.syscall !== 'listen')
        throw error;

    const bind = typeof port === 'string' ? 'Pipe ' + port : 'Port ' + port;

    // Handle specific listen errors with friendly messages.
    switch (error.code) {
        case 'EACCES':
            console.error(bind + ' requires elevated privileges');
            process.exit(1);

            break;
        case 'EADDRINUSE':
            console.error(bind + ' is already in use');
            process.exit(1);

            break;
        default:
            throw error;
    }
};

/**
 * Event listener for HTTP server "listening" event.
 */
const _onListening = (addr) => {
    const bind = typeof addr === 'string' ? 'pipe ' + addr : 'port ' + addr.port;

    console.log('Start listening on ' + bind);
};

/**
 * @param settings
 * @param {ApiServer} apiSrv
 * @param {AgentsHandler} agentsHnd
 * @param {BrowsersHandler} BrowsersHandler
 */
const init = ([settings, apiSrv, agentsHnd, BrowsersHandler]) => {
    // Start rest server.
    const srv = settings.server.SSLOptions ? https.createServer(settings.server.SSLOptions) : http.createServer();

    srv.listen(settings.server.port);

    srv.on('error', _onError.bind(null, settings.server.port));
    srv.on('listening', _onListening.bind(null, srv.address()));

    apiSrv.attach(srv);

    const browsersHnd = new BrowsersHandler();

    agentsHnd.attach(srv, browsersHnd);
    browsersHnd.attach(srv, agentsHnd);

    // Used for automated test.
    if (process.send)
        process.send('running');
};

Promise.all([injector('settings'), injector('api-server'), injector('agents-handler'), injector('browsers-handler')])
    .then(init)
    .catch((err) => {
        console.error(err);

        process.exit(1);
    });
