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

// Fire me up!

module.exports = {
    implements: 'profile-routes',
    inject: ['require(lodash)', 'require(express)', 'mongo']
};

module.exports.factory = function (_, express, mongo) {
    return new Promise((resolve) => {
        const router = express.Router();

        function _updateUser(res, params) {
            mongo.Account.update({_id: params._id}, params, {upsert: true}, function (err, user) {
                // TODO IGNITE-843 Send error to admin.
                if (err)
                    return res.status(500).send('Failed to update profile!');

                if (params.email)
                    user.email = params.email;

                res.sendStatus(200);
            });
        }

        function _checkEmail(res, user, params) {
            if (params.email && user.email != params.email) {
                mongo.Account.findOne({email: params.email}, function (err, userForEmail) {
                    // TODO send error to admin
                    if (err)
                        return res.status(500).send('Failed to check e-mail!');

                    if (userForEmail && userForEmail._id != user._id)
                        return res.status(500).send('User with this e-mail already registered!');

                    _updateUser(res, params);
                });
            }
            else
                _updateUser(res, params);
        }

        /**
         * Save user profile.
         */
        router.post('/save', function (req, res) {
            var params = req.body;

            mongo.Account.findById(params._id, function (err, user) {
                // TODO IGNITE-843 Send error to admin
                if (err)
                    return res.status(500).send('Failed to find user!');

                if (params.password) {
                    if (_.isEmpty(params.password))
                        return res.status(500).send('Wrong value for new password!');

                    user.setPassword(params.password, function (err, user) {
                        if (err)
                            return res.status(500).send(err.message);

                        user.save(function (err) {
                            if (err)
                                return res.status(500).send("Failed to change password!");

                            _checkEmail(res, user, params);
                        });
                    });
                }
                else
                    _checkEmail(res, user, params);
            });
        });

        resolve(router);
    });
};
