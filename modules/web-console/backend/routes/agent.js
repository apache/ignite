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
    implements: 'routes/agents',
    inject: ['require(lodash)', 'require(express)', 'services/agents']
};

/**
 * @param _
 * @param express
 * @param {AgentsService} agentsService
 * @returns {Promise}
 */
module.exports.factory = function(_, express, agentsService) {
    return new Promise((resolveFactory) => {
        const router = new express.Router();

        /* Get grid topology. */
        router.get('/download/zip', (req, res) => {
            const host = req.hostname.match(/:/g) ? req.hostname.slice(0, req.hostname.indexOf(':')) : req.hostname;

            agentsService.getArchive(host, req.user.token)
                .then(({fileName, buffer}) => {
                    // Set the archive name.
                    res.attachment(fileName);

                    res.send(buffer);
                })
                .catch(res.api.error);
        });

        resolveFactory(router);
    });
};
