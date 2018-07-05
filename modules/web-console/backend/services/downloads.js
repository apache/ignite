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
const _ = require('lodash');
const JSZip = require('jszip');

// Fire me up!

module.exports = {
    implements: 'services/agents',
    inject: ['settings', 'agents-handler', 'errors']
};

/**
 * @param settings
 * @param agentsHnd
 * @param errors
 * @returns {DownloadsService}
 */
module.exports.factory = (settings, agentsHnd, errors) => {
    class DownloadsService {
        /**
         * Get agent archive with user agent configuration.
         *
         * @returns {*} - readable stream for further piping. (http://stuk.github.io/jszip/documentation/api_jszip/generate_node_stream.html)
         */
        prepareArchive(host, token) {
            if (_.isEmpty(agentsHnd.currentAgent))
                throw new errors.MissingResourceException('Missing agent zip on server. Please ask webmaster to upload agent zip!');

            const {filePath, fileName} = agentsHnd.currentAgent;

            const folder = path.basename(fileName, '.zip');

            // Read a zip file.
            return new Promise((resolve, reject) => {
                fs.readFile(filePath, (errFs, data) => {
                    if (errFs)
                        reject(new errors.ServerErrorException(errFs));

                    JSZip.loadAsync(data)
                        .then((zip) => {
                            const prop = [];

                            prop.push(`tokens=${token}`);
                            prop.push(`server-uri=${host}`);
                            prop.push('#Uncomment following options if needed:');
                            prop.push('#node-uri=http://localhost:8080');
                            prop.push('#node-login=ignite');
                            prop.push('#node-password=ignite');
                            prop.push('#driver-folder=./jdbc-drivers');

                            zip.file(`${folder}/default.properties`, prop.join('\n'));

                            return zip.generateAsync({type: 'nodebuffer', platform: 'UNIX'})
                                .then((buffer) => resolve({filePath, fileName, buffer}));
                        })
                        .catch(reject);
                });
            });
        }
    }

    return new DownloadsService();
};
