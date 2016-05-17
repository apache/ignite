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
    implements: 'public-routes',
    inject: ['require(express)', 'require(passport)', 'require(nodemailer)', 'settings', 'mail', 'mongo']
};

module.exports.factory = function(express, passport, nodemailer, settings, mail, mongo) {
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
            const becomeUsed = req.session.viewedUser && req.user.admin;

            let user = req.user;

            if (becomeUsed) {
                user = req.session.viewedUser;

                user.becomeUsed = true;
            }
            else if (user)
                user = user.toJSON();
            else
                return res.json(user);

            mongo.Space.findOne({owner: user._id, demo: true}).exec()
                .then((demoSpace) => {
                    if (user && demoSpace)
                        user.demoCreated = true;

                    res.json(user);
                })
                .catch((err) => {
                    res.status(401).send(err.message);
                });
        });

        /**
         * Register new account.
         */
        router.post('/signup', (req, res) => {
            mongo.Account.count().exec()
                .then((cnt) => {
                    req.body.admin = cnt === 0;

                    req.body.token = _randomString();

                    return new mongo.Account(req.body);
                })
                .then((account) => {
                    return new Promise((resolve, reject) => {
                        mongo.Account.register(account, req.body.password, (err, _account) => {
                            if (err)
                                reject(err);

                            if (!_account)
                                reject(new Error('Failed to create account.'));

                            resolve(_account);
                        });
                    });
                })
                .then((account) => new mongo.Space({name: 'Personal space', owner: account._id}).save()
                    .then(() => account)
                )
                .then((account) => {
                    return new Promise((resolve, reject) => {
                        req.logIn(account, {}, (err) => {
                            if (err)
                                reject(err);

                            resolve(account);
                        });
                    });
                })
                .then((account) => {
                    res.sendStatus(200);

                    account.resetPasswordToken = _randomString();

                    return account.save()
                        .then(() => mail.send(account, `Thanks for signing up for ${settings.smtp.username}.`,
                            `Hello ${account.firstName} ${account.lastName}!<br><br>` +
                            `You are receiving this email because you have signed up to use <a href="http://${req.headers.host}">${settings.smtp.username}</a>.<br><br>` +
                            'If you have not done the sign up and do not know what this email is about, please ignore it.<br>' +
                            'You may reset the password by clicking on the following link, or paste this into your browser:<br><br>' +
                            `http://${req.headers.host}/password/reset?token=${account.resetPasswordToken}`));
                })
                .catch((err) => {
                    res.status(401).send(err.message);
                });
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
                .then((user) => mail.send(user, 'Password Reset',
                    `Hello ${user.firstName} ${user.lastName}!<br><br>` +
                    'You are receiving this because you (or someone else) have requested the reset of the password for your account.<br><br>' +
                    'Please click on the following link, or paste this into your browser to complete the process:<br><br>' +
                    'http://' + req.headers.host + '/password/reset?token=' + user.resetPasswordToken + '<br><br>' +
                    'If you did not request this, please ignore this email and your password will remain unchanged.',
                    'Failed to send email with reset link!')
                )
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
                .then((user) => mail.send(user, 'Your password has been changed',
                    `Hello ${user.firstName} ${user.lastName}!<br><br>` +
                    `This is a confirmation that the password for your account on <a href="http://${req.headers.host}">${settings.smtp.username}</a> has just been changed.<br><br>`,
                    'Password was changed, but failed to send confirmation email!'))
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
