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
    implements: 'admin-routes',
    inject: ['require(lodash)', 'require(express)', 'require(nodemailer)', 'settings', 'mongo']
};

module.exports.factory = function(_, express, nodemailer, settings, mongo) {
    return new Promise((resolve) => {
        const router = express.Router();

        /**
         * Get list of user accounts.
         */
        router.post('/list', function(req, res) {
            mongo.Account.find({}).sort('username').exec(function(err, users) {
                if (err)
                    return res.status(500).send(err.message);

                res.json(users);
            });
        });

        // Remove user.
        router.post('/remove', function(req, res) {
            const userId = req.body.userId;

            mongo.Account.findByIdAndRemove(userId, function(errAccount, user) {
                if (errAccount)
                    return res.status(500).send(errAccount.message);

                mongo.Space.find({owner: userId}, function(err, spaces) {
                    _.forEach(spaces, (space) => {
                        mongo.Cluster.remove({space: space._id}).exec();
                        mongo.Cache.remove({space: space._id}).exec();
                        mongo.DomainModel.remove({space: space._id}).exec();
                        mongo.Notebook.remove({space: space._id}).exec();
                        mongo.Space.remove({owner: space._id}).exec();
                    });
                });

                const transporter = {
                    service: settings.smtp.service,
                    auth: {
                        user: settings.smtp.email,
                        pass: settings.smtp.password
                    }
                };

                if (transporter.service !== '' || transporter.auth.user !== '' || transporter.auth.pass !== '') {
                    const mailer = nodemailer.createTransport(transporter);

                    const mailOptions = {
                        from: settings.smtp.address(settings.smtp.username, settings.smtp.email),
                        to: settings.smtp.address(user.username, user.email),
                        subject: 'Your account was deleted',
                        text: 'You are receiving this e-mail because admin remove your account.\n\n' +
                        '--------------\n' +
                        'Apache Ignite Web Console http://' + req.headers.host + '\n'
                    };

                    mailer.sendMail(mailOptions, function(errMailer) {
                        if (errMailer)
                            return res.status(503).send('Account was removed, but failed to send e-mail notification to user!<br />' + errMailer);

                        res.sendStatus(200);
                    });
                }
                else
                    res.sendStatus(200);
            });
        });

        // Save user.
        router.post('/save', function(req, res) {
            const userId = req.body.userId;
            const adminFlag = req.body.adminFlag;

            mongo.Account.findByIdAndUpdate(userId, {admin: adminFlag}, function(err) {
                if (err)
                    return res.status(500).send(err.message);

                res.sendStatus(200);
            });
        });

        // Become user.
        router.get('/become', function(req, res) {
            mongo.Account.findById(req.query.viewedUserId).exec(function(err, viewedUser) {
                if (err)
                    return res.sendStatus(404);

                req.session.viewedUser = viewedUser;

                return res.sendStatus(200);
            });
        });

        // Become user.
        router.get('/revert/identity', function(req, res) {
            req.session.viewedUser = null;

            return res.sendStatus(200);
        });

        resolve(router);
    });
};

