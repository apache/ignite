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
            mongoUrl: nconf.get('mongodb:url') || 'mongodb://127.0.0.1/console',
            cookieTTL: 3600000 * 24 * 30,
            sessionSecret: nconf.get('server:sessionSecret') || 'keyboard cat',
            tokenLength: 20
        };

        // Configure SSL options.
        if (_isTrue('server:ssl')) {
            const SSLOptions = {
                enable301Redirects: true,
                trustXFPHeader: true
            };

            const setSslOption = (cfg, isFile = false) => {
                const cfgValue = nconf.get(`server:${cfg}`);

                const hasOption = !!cfgValue;

                if (hasOption)
                    SSLOptions[cfg] = isFile ?  fs.readFileSync(cfgValue) : cfgValue;

                return hasOption;
            };

            setSslOption('key', true);
            setSslOption('cert', true);
            setSslOption('ca', true);
            setSslOption('passphrase');
            setSslOption('secureProtocol');

            if (setSslOption('ciphers'))
                SSLOptions.honorCipherOrder = true;

            settings.server.SSLOptions = SSLOptions;
        }

        return settings;
    }
};
