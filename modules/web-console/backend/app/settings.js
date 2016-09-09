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

// Fire me up!

/**
 * Module with server-side configuration.
 */
module.exports = {
    implements: 'settings',
    inject: ['nconf', 'require(fs)']
};

module.exports.factory = function(nconf, fs) {
    /**
     * Normalize a port into a number, string, or false.
     */
    const _normalizePort = function(val) {
        const port = parseInt(val, 10);

        // named pipe
        if (isNaN(port))
            return val;

        // port number
        if (port >= 0)
            return port;

        return false;
    };

    const mailConfig = nconf.get('mail') || {};

    return {
        agent: {
            dists: 'agent_dists',
            port: _normalizePort(nconf.get('agentServer:port') || 3002),
            legacyPort: _normalizePort(nconf.get('agentServer:legacyPort')),
            SSLOptions: nconf.get('agentServer:ssl') && {
                key: fs.readFileSync(nconf.get('agentServer:key')),
                cert: fs.readFileSync(nconf.get('agentServer:cert')),
                passphrase: nconf.get('agentServer:keyPassphrase')
            }
        },
        server: {
            port: _normalizePort(nconf.get('server:port') || 3000),
            SSLOptions: nconf.get('server:ssl') && {
                enable301Redirects: true,
                trustXFPHeader: true,
                key: fs.readFileSync(nconf.get('server:key')),
                cert: fs.readFileSync(nconf.get('server:cert')),
                passphrase: nconf.get('server:keyPassphrase')
            }
        },
        smtp: {
            ...mailConfig,
            address: (username, email) => username ? '"' + username + '" <' + email + '>' : email
        },
        mongoUrl: nconf.get('mongodb:url') || 'mongodb://localhost/console',
        cookieTTL: 3600000 * 24 * 30,
        sessionSecret: nconf.get('server:sessionSecret') || 'keyboard cat',
        tokenLength: 20
    };
};
