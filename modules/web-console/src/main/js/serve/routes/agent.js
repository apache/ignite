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

module.exports = {
    implements: 'agent-routes',
    inject: ['require(lodash)', 'require(express)', 'require(fs)', 'require(jszip)', 'settings', 'agent-manager']
};

/**
 * @param _
 * @param express
 * @param fs
 * @param JSZip
 * @param settings
 * @param {AgentManager} agentMgr
 * @returns {Promise}
 */
module.exports.factory = function(_, express, fs, JSZip, settings, agentMgr) {
    return new Promise((resolveFactory) => {
        const router = new express.Router();

        /* Get grid topology. */
        router.get('/download/zip', (req, res) => {
            const latest = agentMgr.supportedAgents.latest;

            if (_.isEmpty(latest))
                return res.status(500).send('Missing agent zip on server. Please ask webmaster to upload agent zip!');

            const agentFld = latest.fileName.substr(0, latest.fileName.length - 4);
            const agentZip = latest.fileName;
            const agentPathZip = latest.filePath;

            // Read a zip file.
            fs.readFile(agentPathZip, (errFs, data) => {
                if (errFs)
                    return res.download(agentPathZip, agentZip);

                // Set the archive name.
                res.attachment(agentZip);

                JSZip.loadAsync(data)
                    .then((zip) => {
                        const prop = [];

                        const host = req.hostname.match(/:/g) ? req.hostname.slice(0, req.hostname.indexOf(':')) : req.hostname;

                        prop.push('tokens=' + req.user.token);
                        prop.push('server-uri=' + (settings.agent.SSLOptions ? 'https' : 'http') + '://' + host + ':' + settings.agent.port);
                        prop.push('#Uncomment following options if needed:');
                        prop.push('#node-uri=http://localhost:8080');
                        prop.push('#driver-folder=./jdbc-drivers');

                        zip.file(agentFld + '/default.properties', prop.join('\n'));

                        zip.generateAsync({type: 'nodebuffer', platform: 'UNIX'})
                            .then((buffer) => res.send(buffer));
                    });
            });
        });

        resolveFactory(router);
    });
};
