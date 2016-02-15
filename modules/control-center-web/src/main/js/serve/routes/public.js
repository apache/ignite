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

        /**
         * Send mail to user.
         * @private
         * @return {Promise}
         */
        const _sendMail = (user, subject, text, sendErrMsg) => {
            const transporter = {
                service: settings.smtp.service,
                auth: {
                    user: settings.smtp.email,
                    pass: settings.smtp.password
                }
            };

            if (transporter.service === '' || transporter.auth.user === '' || transporter.auth.pass === '')
                return Promise.reject('Can\'t send e-mail because not configured SMTP server. Please ask webmaster to setup SMTP server!');

            const mailer = nodemailer.createTransport(transporter);

            const mail = {
                from: settings.smtp.address(settings.smtp.username, settings.smtp.email),
                to: settings.smtp.address(user.username, user.email),
                subject,
                text: text + (settings.smtp.username ? `\n\n--------------\n${settings.smtp.username}\n` : '')
            };

            return new Promise((resolve, reject) => {
                mailer.sendMail(mail, (err) => {
                    if (err)
                        return reject(sendErrMsg || err.message);

                    resolve(user);
                });
            });
        };

        // GET user.
        router.post('/user', (req, res) => {
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
        router.post('/signup', (req, res) => {
            mongo.Account.count().exec()
                .then((cnt) => {
                    req.body.admin = cnt === 0;

                    req.body.token = _randomString();

                    return Promise.resolve(new mongo.Account(req.body));
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
                .then((account) => {
                    return new Promise((resolve, reject) =>
                        new mongo.Space({name: 'Personal space', owner: account._id}).save()
                            .then(() => resolve(account))
                            .catch(reject)
                    );
                })
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

                    account.save()
                        .then(() =>
                            _sendMail(account, `Thanks for signing up for ${settings.smtp.username}.`,
                                `Hello ${account.username}!\n\n` +
                                `You are receiving this e-mail because you (or someone else) signing up on the ${settings.smtp.username}.\n\n` +
                                'If you did not request this, please ignore this email.\n' +
                                'You may reset password by clicking on the following link, or paste this into your browser:\n\n' +
                                'http://' + req.headers.host + '/password/reset?token=' + account.resetPasswordToken));
                })
                .catch((err) => {
                    res.status(401).send(err.message);
                });
        });

        /**
         * Login in exist account.
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
                        return Promise.reject('Account with that email address does not exists!');

                    user.resetPasswordToken = _randomString();

                    return user.save();
                })
                .then((user) =>
                    _sendMail(user, 'Password Reset',
                        'You are receiving this because you (or someone else) have requested the reset of the password for your account.\n\n' +
                        'Please click on the following link, or paste this into your browser to complete the process:\n\n' +
                        'http://' + req.headers.host + '/password/reset?token=' + user.resetPasswordToken + '\n\n' +
                        'If you did not request this, please ignore this email and your password will remain unchanged.',
                        'Failed to send e-mail with reset link!')
                )
                .then(() => res.status(200).send('An e-mail has been sent with further instructions.'))
                .catch((errMsg) => {
                    // TODO IGNITE-843 Send email to admin
                    return res.status(401).send(errMsg);
                });
        });

        /**
         * Change password with given token.
         */
        router.post('/password/reset', (req, res) => {
            mongo.Account.findOne({resetPasswordToken: req.body.token}).exec()
                .then((user) => {
                    return new Promise((resolve, reject) => {
                        if (!user)
                            return reject('Failed to find account with this token! Please check link from e-mail.');

                        user.setPassword(req.body.password, (err, _user) => {
                            if (err)
                                return reject('Failed to reset password: ' + err.message);

                            _user.resetPasswordToken = undefined; // eslint-disable-line no-undefined

                            resolve(_user.save());
                        });
                    });
                })
                .then((user) => {
                    return _sendMail(user, 'Your password has been changed',
                        'Hello,\n\n' +
                        'This is a confirmation that the password for your account ' + user.email + ' has just been changed.\n\n' +
                        'Now you can login: http://' + req.headers.host,
                        'Password was changed, but failed to send confirmation e-mail!');
                })
                .then((user) => res.status(200).send(user.email))
                .catch((errMsg) => {
                    res.status(500).send(errMsg);
                });
        });

        /* GET reset password page. */
        router.post('/password/validate/token', (req, res) => {
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
