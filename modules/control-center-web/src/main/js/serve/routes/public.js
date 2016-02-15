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
    inject: ['require(express)', 'require(passport)', 'require(nodemailer)', 'settings', 'mongo']
};

module.exports.factory = function(express, passport, nodemailer, settings, mongo) {
    return new Promise(function(factoryResolve) {
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
        router.post('/user', function(req, res) {
            const becomeUsed = req.session.viewedUser && req.user.admin;

            let user = req.user;

            if (becomeUsed) {
                user = req.session.viewedUser;

                user.becomeUsed = true;
            }

            res.json(user);
        });

        /**
         * Register new account.
         */
        router.post('/register', function(req, res) {
            mongo.Account.count(function(errCount, cnt) {
                if (errCount)
                    return res.status(401).send(errCount.message);

                req.body.admin = cnt === 0;

                const _account = new mongo.Account(req.body);

                _account.token = _randomString();

                mongo.Account.register(_account, req.body.password, function(errRegister, account) {
                    if (errRegister)
                        return res.status(401).send(errRegister.message);

                    if (!account)
                        return res.status(500).send('Failed to create account.');

                    new mongo.Space({name: 'Personal space', owner: account._id}).save();

                    req.logIn(account, {}, function(errLogIn) {
                        if (errLogIn)
                            return res.status(401).send(errLogIn.message);

                        return res.sendStatus(200);
                    });
                });
            });
        });

        /**
         * Login in exist account.
         */
        router.post('/login', function(req, res, next) {
            passport.authenticate('local', function(errAuth, user) {
                if (errAuth)
                    return res.status(401).send(errAuth.message);

                if (!user)
                    return res.status(401).send('Invalid email or password');

                req.logIn(user, {}, function(errLogIn) {
                    if (errLogIn)
                        return res.status(401).send(errLogIn.message);

                    return res.sendStatus(200);
                });
            })(req, res, next);
        });

        /**
         * Logout.
         */
        router.post('/logout', function(req, res) {
            req.logout();

            res.sendStatus(200);
        });

        /**
         * Send e-mail to user with reset token.
         */
        router.post('/password/forgot', function(req, res) {
            const transporter = {
                service: settings.smtp.service,
                auth: {
                    user: settings.smtp.email,
                    pass: settings.smtp.password
                }
            };

            if (transporter.service === '' || transporter.auth.user === '' || transporter.auth.pass === '')
                return res.status(401).send('Can\'t send e-mail with instructions to reset password. Please ask webmaster to setup SMTP server!');

            const token = _randomString();

            mongo.Account.findOne({email: req.body.email}).exec()
                .then((user) => {
                    if (!user)
                        return Promise.reject('Account with that email address does not exists!');

                    user.resetPasswordToken = token;

                    return user.save();
                })
                .then((user) => {
                    const mailer = nodemailer.createTransport(transporter);

                    const mailOptions = {
                        from: settings.smtp.address(settings.smtp.username, settings.smtp.email),
                        to: settings.smtp.address(user.username, user.email),
                        subject: 'Password Reset',
                        text: 'You are receiving this because you (or someone else) have requested the reset of the password for your account.\n\n' +
                        'Please click on the following link, or paste this into your browser to complete the process:\n\n' +
                        'http://' + req.headers.host + '/password/reset?token=' + token + '\n\n' +
                        'If you did not request this, please ignore this email and your password will remain unchanged.\n\n' +
                        '--------------\n' +
                        settings.smtp.username + '\n'
                    };

                    return new Promise((resolve, reject) => {
                        mailer.sendMail(mailOptions, (err) => {
                            if (err)
                                return reject();

                            resolve(res.status(200).send('An e-mail has been sent with further instructions.'));
                        });
                    });
                })
                .catch((err) => {
                    // TODO IGNITE-843 Send email to admin
                    return res.status(401).send('Failed to send e-mail with reset link!' + (err ? '<br>' + err : ''));
                });
        });

        /**
         * Change password with given token.
         */
        router.post('/password/reset', function(req, res) {
            mongo.Account.findOne({resetPasswordToken: req.body.token}).exec()
                .then((user) => {
                    return new Promise((resolve, reject) => {
                        if (!user)
                            return reject();

                        user.setPassword(req.body.password, (err, _user) => {
                            if (err) {
                                console.log('Failed to reset password: ' + err.message);

                                return reject();
                            }

                            _user.resetPasswordToken = undefined; // eslint-disable-line no-undefined

                            resolve(_user.save());
                        });
                    });
                })
                .then((user) => {
                    const transporter = {
                        service: settings.smtp.service,
                        auth: {
                            user: settings.smtp.email,
                            pass: settings.smtp.password
                        }
                    };

                    const mailer = nodemailer.createTransport(transporter);

                    const mailOptions = {
                        from: settings.smtp.address(settings.smtp.username, settings.smtp.email),
                        to: settings.smtp.address(user.username, user.email),
                        subject: 'Your password has been changed',
                        text: 'Hello,\n\n' +
                        'This is a confirmation that the password for your account ' + user.email + ' has just been changed.\n\n' +
                        'Now you can login: http://' + req.headers.host + '\n\n' +
                        '--------------\n' +
                        'Apache Ignite Web Console\n'
                    };

                    return new Promise((resolve) => {
                        mailer.sendMail(mailOptions, (err) => {
                            if (err)
                                return resolve(res.status(500).send('Password was changed, but failed to send confirmation e-mail!'));

                            resolve(res.status(200).send(user.email));
                        });
                    });
                })
                .catch(() => {
                    res.status(500).send('Failed to reset password! Please check link from e-mail.');
                });
        });

        /* GET reset password page. */
        router.post('/validate/token', function(req, res) {
            const token = req.body.token;

            mongo.Account.findOne({resetPasswordToken: token}).exec()
                .then((user) => {
                    return new Promise((resolve, reject) => {
                        if (!user)
                            return reject('Invalid token for password reset!');

                        resolve(res.json({token, email: user.email}));
                    });
                })
                .catch((error) => res.json({error}));
        });

        factoryResolve(router);
    });
};
