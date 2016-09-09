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
    implements: 'routes/public',
    inject: ['require(express)', 'require(passport)', 'settings', 'mongo', 'services/mails', 'services/users']
};

/**
 *
 * @param express
 * @param passport
 * @param settings
 * @param mongo
 * @param mailsService
 * @param {UsersService} usersService
 * @returns {Promise}
 */
module.exports.factory = function(express, passport, settings, mongo, mailsService, usersService) {
    return new Promise((factoryResolve) => {
        const router = new express.Router();

        const _randomString = () => {
            const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
            const possibleLen = possible.length;

            let res = '';

            for (let i = 0; i < settings.tokenLength; i++)
                res += possible.charAt(Math.floor(Math.random() * possibleLen));

            return res;
        };

        // GET user.
        router.post('/user', (req, res) => {
            usersService.get(req.user, req.session.viewedUser)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        /**
         * Register new account.
         */
        router.post('/signup', (req, res) => {
            usersService.create(req.origin(), req.body)
                .then((user) => new Promise((resolve, reject) => {
                    req.logIn(user, {}, (err) => {
                        if (err)
                            reject(err);

                        resolve(user);
                    });
                }))
                .then(res.api.ok)
                .catch(res.api.error);
        });

        /**
         * Sign in into exist account.
         */
        router.post('/signin', (req, res, next) => {
            passport.authenticate('local', (errAuth, user) => {
                if (errAuth)
                    return res.status(401).send(errAuth.message);

                if (!user)
                    return res.status(401).send('Invalid email or password');

                req.logIn(user, {}, (errLogIn) => {
                    if (errLogIn)
                        return res.status(401).send(errLogIn.message);

                    return res.sendStatus(200);
                });
            })(req, res, next);
        });

        /**
         * Logout.
         */
        router.post('/logout', (req, res) => {
            req.logout();

            res.sendStatus(200);
        });

        /**
         * Send e-mail to user with reset token.
         */
        router.post('/password/forgot', (req, res) => {
            mongo.Account.findOne({email: req.body.email}).exec()
                .then((user) => {
                    if (!user)
                        throw new Error('Account with that email address does not exists!');

                    user.resetPasswordToken = _randomString();

                    return user.save();
                })
                .then((user) => mailsService.emailUserResetLink(req.origin(), user))
                .then(() => res.status(200).send('An email has been sent with further instructions.'))
                .catch((err) => {
                    // TODO IGNITE-843 Send email to admin
                    return res.status(401).send(err.message);
                });
        });

        /**
         * Change password with given token.
         */
        router.post('/password/reset', (req, res) => {
            mongo.Account.findOne({resetPasswordToken: req.body.token}).exec()
                .then((user) => {
                    if (!user)
                        throw new Error('Failed to find account with this token! Please check link from email.');

                    return new Promise((resolve, reject) => {
                        user.setPassword(req.body.password, (err, _user) => {
                            if (err)
                                return reject(new Error('Failed to reset password: ' + err.message));

                            _user.resetPasswordToken = undefined; // eslint-disable-line no-undefined

                            resolve(_user.save());
                        });
                    });
                })
                .then((user) => mailsService.emailPasswordChanged(req.origin(), user))
                .then((user) => res.status(200).send(user.email))
                .catch((err) => res.status(401).send(err.message));
        });

        /* GET reset password page. */
        router.post('/password/validate/token', (req, res) => {
            const token = req.body.token;

            mongo.Account.findOne({resetPasswordToken: token}).exec()
                .then((user) => {
                    if (!user)
                        throw new Error('Invalid token for password reset!');

                    return res.json({token, email: user.email});
                })
                .catch((err) => res.status(401).send(err.message));
        });

        factoryResolve(router);
    });
};
