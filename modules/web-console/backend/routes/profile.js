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

const express = require('express');
const _ = require('lodash');

// Fire me up!

module.exports = {
    implements: 'routes/profiles',
    inject: ['mongo', 'services/users']
};

/**
 * @param mongo
 * @param {UsersService} usersService
 * @returns {Promise}
 */
module.exports.factory = function(mongo, usersService) {
    return new Promise((resolveFactory) => {
        const router = new express.Router();

        /**
         * Save user profile.
         */
        router.post('/save', (req, res) => {
            if (req.body.password && _.isEmpty(req.body.password))
                return res.status(500).send('Wrong value for new password!');

            usersService.save(req.body)
                .then((user) => {
                    const becomeUsed = req.session.viewedUser && req.user.admin;

                    if (becomeUsed) {
                        req.session.viewedUser = user;

                        return req.user;
                    }

                    return new Promise((resolve, reject) => {
                        req.logout();

                        req.logIn(user, {}, (errLogIn) => {
                            if (errLogIn)
                                return reject(errLogIn);

                            return resolve(user);
                        });
                    });
                })
                .then((user) => usersService.get(user, req.session.viewedUser))
                .then(res.api.ok)
                .catch(res.api.error);
        });

        resolveFactory(router);
    });
};
