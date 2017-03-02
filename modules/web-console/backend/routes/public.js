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
    inject: ['require(express)', 'require(passport)', 'mongo', 'services/mails', 'services/users', 'services/auth']
};

/**
 * @param express
 * @param passport
 * @param mongo
 * @param mailsService
 * @param {UsersService} usersService
 * @param {AuthService} authService
 * @returns {Promise}
 */
module.exports.factory = function(express, passport, mongo, mailsService, usersService, authService) {
    return new Promise((factoryResolve) => {
        const router = new express.Router();

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
            authService.resetPasswordToken(req.body.email)
                .then((user) => mailsService.emailUserResetLink(req.origin(), user))
                .then(() => 'An email has been sent with further instructions.')
                .then(res.api.ok)
                .catch(res.api.error);
        });

        /**
         * Change password with given token.
         */
        router.post('/password/reset', (req, res) => {
            const {token, password} = req.body;

            authService.resetPasswordByToken(token, password)
                .then((user) => mailsService.emailPasswordChanged(req.origin(), user))
                .then((user) => user.email)
                .then(res.api.ok)
                .then(res.api.error);
        });

        /* GET reset password page. */
        router.post('/password/validate/token', (req, res) => {
            const token = req.body.token;

            authService.validateResetToken(token)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        factoryResolve(router);
    });
};
