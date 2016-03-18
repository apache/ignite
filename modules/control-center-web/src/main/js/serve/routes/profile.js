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
    implements: 'profile-routes',
    inject: ['require(lodash)', 'require(express)', 'mongo', 'agent']
};

/**
 *
 * @param _
 * @param express
 * @param mongo
 * @param {AgentManager} agent
 * @returns {Promise}
 */
module.exports.factory = function(_, express, mongo, agent) {
    return new Promise((resolveFactory) => {
        const router = new express.Router();

        /**
         * Save user profile.
         */
        router.post('/save', (req, res) => {
            const params = req.body;

            if (params.password && _.isEmpty(params.password))
                return res.status(500).send('Wrong value for new password!');

            mongo.Account.findById(params._id).exec()
                .then((user) => {
                    if (!params.password)
                        return Promise.resolve(user);

                    return new Promise((resolve, reject) => {
                        user.setPassword(params.password, (err, _user) => {
                            if (err)
                                return reject(err);

                            delete params.password;

                            resolve(_user);
                        });
                    });
                })
                .then((user) => {
                    if (!params.email || user.email === params.email)
                        return Promise.resolve(user);

                    return new Promise((resolve, reject) => {
                        mongo.Account.findOne({email: params.email}, (err, _user) => {
                            // TODO send error to admin
                            if (err)
                                reject(new Error('Failed to check email!'));

                            if (_user && _user._id !== user._id)
                                reject(new Error('User with this email already registered!'));

                            resolve(user);
                        });
                    });
                })
                .then((user) => {
                    if (!params.token || user.token !== params.token)
                        agent.close(user._id);

                    for (const param in params) {
                        if (params.hasOwnProperty(param))
                            user[param] = params[param];
                    }

                    return user.save();
                })
                .then(() => res.sendStatus(200))
                .catch((err) => mongo.handleError(res, err));
        });

        resolveFactory(router);
    });
};
