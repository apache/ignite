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

module.exports.factory = function (express, passport, nodemailer, settings, mongo) {
    return new Promise(function (resolve) {
        const router = express.Router();

        const _randomString = () => {
            const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
            const possibleLen = possible.length;

            let res = '';

            for (let i = 0; i < settings.tokenLength; i++)
                res += possible.charAt(Math.floor(Math.random() * possibleLen));

            return res;
        };

        // GET user.
        router.post('/user', function (req, res) {
            var becomeUsed = req.session.viewedUser && req.user.admin;

            var user = req.user;

            if (becomeUsed) {
                user = req.session.viewedUser;

                user.becomeUsed = true;
            }

            res.json(user);
        });

        /**
         * Register new account.
         */
        router.post('/register', function (req, res) {
            mongo.Account.count(function (err, cnt) {
                if (err)
                    return res.status(401).send(err.message);

                req.body.admin = cnt == 0;

                var account = new mongo.Account(req.body);

                account.token = _randomString();

                mongo.Account.register(account, req.body.password, function (err, account) {
                    if (err)
                        return res.status(401).send(err.message);

                    if (!account)
                        return res.status(500).send('Failed to create account.');

                    new mongo.Space({name: 'Personal space', owner: account._id}).save();

                    req.logIn(account, {}, function (err) {
                        if (err)
                            return res.status(401).send(err.message);

                        return res.sendStatus(200);
                    });
                });
            });
        });

        /**
         * Login in exist account.
         */
        router.post('/login', function (req, res, next) {
            passport.authenticate('local', function (err, user) {
                if (err)
                    return res.status(401).send(err.message);

                if (!user)
                    return res.status(401).send('Invalid email or password');

                req.logIn(user, {}, function (err) {
                    if (err)
                        return res.status(401).send(err.message);

                    return res.sendStatus(200);
                });
            })(req, res, next);
        });

        /**
         * Logout.
         */
        router.post('/logout', function (req, res) {
            req.logout();

            res.sendStatus(200);
        });

        /**
         * Send e-mail to user with reset token.
         */
        router.post('/password/forgot', function (req, res) {
            var transporter = {
                service: settings.smtp.service,
                auth: {
                    user: settings.smtp.email,
                    pass: settings.smtp.password
                }
            };

            if (transporter.service == '' || transporter.auth.user == '' || transporter.auth.pass == '')
                return res.status(401).send('Can\'t send e-mail with instructions to reset password. Please ask webmaster to setup SMTP server!');

            var token = _randomString();

            mongo.Account.findOne({email: req.body.email}, function (err, user) {
                if (!user)
                    return res.status(401).send('No account with that email address exists!');

                if (err)
                // TODO IGNITE-843 Send email to admin
                    return res.status(401).send('Failed to reset password!');

                user.resetPasswordToken = token;

                user.save(function (err) {
                    if (err)
                    // TODO IGNITE-843 Send email to admin
                        return res.status(401).send('Failed to reset password!');

                    var mailer = nodemailer.createTransport(transporter);

                    var mailOptions = {
                        from: settings.smtp.address(settings.smtp.username, settings.smtp.email),
                        to: settings.smtp.address(user.username, user.email),
                        subject: 'Password Reset',
                        text: 'You are receiving this because you (or someone else) have requested the reset of the password for your account.\n\n' +
                        'Please click on the following link, or paste this into your browser to complete the process:\n\n' +
                        'http://' + req.headers.host + '/password/reset?token=' + token + '\n\n' +
                        'If you did not request this, please ignore this email and your password will remain unchanged.\n\n' +
                        '--------------\n' +
                        'Apache Ignite Web Console\n'
                    };

                    mailer.sendMail(mailOptions, function (err) {
                        if (err)
                            return res.status(401).send('Failed to send e-mail with reset link! ' + err);

                        return res.status(200).send('An e-mail has been sent with further instructions.');
                    });
                });
            });
        });

        /**
         * Change password with given token.
         */
        router.post('/password/reset', function (req, res) {
            mongo.Account.findOne({resetPasswordToken: req.body.token}, function (err, user) {
                if (!user)
                    return res.status(500).send('Invalid token for password reset!');

                // TODO IGNITE-843 Send email to admin
                if (err)
                    return res.status(500).send('Failed to reset password!');

                user.setPassword(req.body.password, function (err, updatedUser) {
                    if (err)
                        return res.status(500).send(err.message);

                    updatedUser.resetPasswordToken = undefined;

                    updatedUser.save(function (err) {
                        if (err)
                            return res.status(500).send(err.message);

                        var transporter = {
                            service: settings.smtp.service,
                            auth: {
                                user: settings.smtp.email,
                                pass: settings.smtp.password
                            }
                        };

                        var mailer = nodemailer.createTransport(transporter);

                        var mailOptions = {
                            from: settings.smtp.address(settings.smtp.username, settings.smtp.email),
                            to: settings.smtp.address(user.username, user.email),
                            subject: 'Your password has been changed',
                            text: 'Hello,\n\n' +
                            'This is a confirmation that the password for your account ' + user.email + ' has just been changed.\n\n' +
                            'Now you can login: http://' + req.headers.host + '\n\n' +
                            '--------------\n' +
                            'Apache Ignite Web Console\n'
                        };

                        mailer.sendMail(mailOptions, function (err) {
                            if (err)
                                return res.status(503).send('Password was changed, but failed to send confirmation e-mail!<br />' + err);

                            return res.status(200).send(user.email);
                        });
                    });
                });
            });
        });

        /* GET reset password page. */
        router.post('/validate/token', function (req, res) {
            var token = req.body.token;

            var data = {token: token};

            mongo.Account.findOne({resetPasswordToken: token}, function (err, user) {
                if (!user)
                    data.error = 'Invalid token for password reset!';
                else if (err)
                    data.error = err;
                else
                    data.email = user.email;

                res.json(data);
            });
        });

        resolve(router);
    });
};
