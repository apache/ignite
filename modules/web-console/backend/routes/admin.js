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

// Fire me up!

module.exports = {
    implements: 'routes/admin',
    inject: ['settings', 'mongo', 'services/spaces', 'services/mails', 'services/sessions', 'services/users', 'services/notifications']
};

/**
 * @param settings
 * @param mongo
 * @param spacesService
 * @param {MailsService} mailsService
 * @param {SessionsService} sessionsService
 * @param {UsersService} usersService
 * @param {NotificationsService} notificationsService
 * @returns {Promise}
 */
module.exports.factory = function(settings, mongo, spacesService, mailsService, sessionsService, usersService, notificationsService) {
    return new Promise((factoryResolve) => {
        const router = new express.Router();

        /**
         * Get list of user accounts.
         */
        router.post('/list', (req, res) => {
            usersService.list(req.body)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        // Remove user.
        router.post('/remove', (req, res) => {
            usersService.remove(req.origin(), req.body.userId)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        // Grant or revoke admin access to user.
        router.post('/toggle', (req, res) => {
            const params = req.body;

            mongo.Account.findByIdAndUpdate(params.userId, {admin: params.adminFlag}).exec()
                .then(res.api.ok)
                .catch(res.api.error);
        });

        // Become user.
        router.get('/become', (req, res) => {
            sessionsService.become(req.session, req.query.viewedUserId)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        // Revert to your identity.
        router.get('/revert/identity', (req, res) => {
            sessionsService.revert(req.session)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        // Update notifications.
        router.put('/notifications', (req, res) => {
            notificationsService.merge(req.user._id, req.body.message, req.body.isShown)
                .then(res.api.done)
                .catch(res.api.error);
        });

        factoryResolve(router);
    });
};

