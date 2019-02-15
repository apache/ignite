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

const express = require('express');
const _ = require('lodash');

// Fire me up!

module.exports = {
    implements: 'routes/notebooks',
    inject: ['mongo', 'services/spaces', 'services/notebooks']
};

module.exports.factory = (mongo, spacesService, notebooksService) => {
    return new Promise((factoryResolve) => {
        const router = new express.Router();

        /**
         * Get notebooks names accessed for user account.
         *
         * @param req Request.
         * @param res Response.
         */
        router.get('/', (req, res) => {
            return spacesService.spaces(req.currentUserId())
                .then((spaces) => _.map(spaces, (space) => space._id))
                .then((spaceIds) => notebooksService.listBySpaces(spaceIds))
                .then(res.api.ok)
                .catch(res.api.error);
        });

        /**
         * Save notebook accessed for user account.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/save', (req, res) => {
            const notebook = req.body;

            spacesService.spaceIds(req.currentUserId())
                .then((spaceIds) => {
                    notebook.space = notebook.space || spaceIds[0];

                    return notebooksService.merge(notebook);
                })
                .then(res.api.ok)
                .catch(res.api.error);
        });

        /**
         * Remove notebook by ._id.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/remove', (req, res) => {
            const notebookId = req.body._id;

            notebooksService.remove(notebookId)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        factoryResolve(router);
    });
};
