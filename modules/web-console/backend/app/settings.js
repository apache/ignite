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
const _ = require('lodash');

// Fire me up!

/**
 * Module with server-side configuration.
 */
module.exports = {
    implements: 'settings',
    inject: ['nconf'],
    factory(nconf) {
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

        const mail = nconf.get('mail') || {};

        const packaged = __dirname.startsWith('/snapshot/') || __dirname.startsWith('C:\\snapshot\\');

        const dfltAgentDists = packaged ? 'libs/agent_dists' : 'agent_dists';
        const dfltHost = packaged ? '0.0.0.0' : '127.0.0.1';
        const dfltPort = packaged ? 80 : 3000;

        // We need this function because nconf() can return String or Boolean.
        // And in JS we cannot compare String with Boolean.
        const _isTrue = (confParam) => {
            const v = nconf.get(confParam);

            return v === 'true' || v === true;
        };

        let activationEnabled = _isTrue('activation:enabled');

        if (activationEnabled && _.isEmpty(mail)) {
            activationEnabled = false;

            console.warn('Mail server settings are required for account confirmation!');
        }

        const settings = {
            agent: {
                dists: nconf.get('agent:dists') || dfltAgentDists
            },
            packaged,
            server: {
                host: nconf.get('server:host') || dfltHost,
                port: _normalizePort(nconf.get('server:port') || dfltPort),
                disableSignup: _isTrue('server:disable:signup')
            },
            mail,
            activation: {
                enabled: activationEnabled,
                timeout: nconf.get('activation:timeout') || 1800000,
                sendTimeout: nconf.get('activation:sendTimeout') || 180000
            },
            mongoUrl: nconf.get('mongodb:url') || 'mongodb://127.0.0.1/console',
            cookieTTL: 3600000 * 24 * 30,
            sessionSecret: nconf.get('server:sessionSecret') || 'keyboard cat',
            tokenLength: 20
        };

        // Configure SSL options.
        if (_isTrue('server:ssl')) {
            const sslOptions = {
                enable301Redirects: true,
                trustXFPHeader: true,
                isServer: true
            };

            const setSslOption = (name, fromFile = false) => {
                const v = nconf.get(`server:${name}`);

                const hasOption = !!v;

                if (hasOption)
                    sslOptions[name] = fromFile ? fs.readFileSync(v) : v;

                return hasOption;
            };

            const setSslOptionBoolean = (name) => {
                const v = nconf.get(`server:${name}`);

                if (v)
                    sslOptions[name] = v === 'true' || v === true;
            };

            setSslOption('key', true);
            setSslOption('cert', true);
            setSslOption('ca', true);
            setSslOption('passphrase');
            setSslOption('ciphers');
            setSslOption('secureProtocol');
            setSslOption('clientCertEngine');
            setSslOption('pfx', true);
            setSslOption('crl');
            setSslOption('dhparam');
            setSslOption('ecdhCurve');
            setSslOption('maxVersion');
            setSslOption('minVersion');
            setSslOption('secureOptions');
            setSslOption('sessionIdContext');

            setSslOptionBoolean('honorCipherOrder');
            setSslOptionBoolean('requestCert');
            setSslOptionBoolean('rejectUnauthorized');

            // Special care for case, when user set password for something like "123456".
            if (sslOptions.passphrase)
                sslOptions.passphrase = sslOptions.passphrase.toString();

            settings.server.SSLOptions = sslOptions;
        }

        return settings;
    }
};
