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
    inject: ['require(lodash)', 'require(express)', 'settings', 'mail', 'mongo']
};

module.exports.factory = function(_, express, settings, mail, mongo) {
    return new Promise((factoryResolve) => {
        const router = new express.Router();

        /**
         * Get list of user accounts.
         */
        router.post('/list', (req, res) => {

            Promise.all([
                mongo.Space.aggregate([
                    {$match: {demo: false}},
                    {$lookup: {from: 'clusters', localField: '_id', foreignField: 'space', as: 'clusters'}},
                    {$lookup: {from: 'caches', localField: '_id', foreignField: 'space', as: 'caches'}},
                    {$lookup: {from: 'domainmodels', localField: '_id', foreignField: 'space', as: 'domainmodels'}},
                    {$lookup: {from: 'igfs', localField: '_id', foreignField: 'space', as: 'igfs'}},
                    {$project: {
                        owner: 1,
                        clusters: {$size: '$clusters'},
                        models: {$size: '$domainmodels'},
                        caches: {$size: '$caches'},
                        igfs: {$size: '$igfs'}
                    }}
                ]).exec(),
                mongo.Account.find({}).sort('firstName lastName').lean().exec()
            ])
            .then((values) => {
                const counters = _.keyBy(values[0], 'owner');
                const accounts = values[1];

                return accounts.map((account) => {
                    account.counters = _.omit(counters[account._id], '_id', 'owner');

                    return account;
                });
            })
            .then((users) => res.json(users))
            .catch((err) => mongo.handleError(res, err));
        });

        // Remove user.
        router.post('/remove', (req, res) => {
            const userId = req.body.userId;

            mongo.Account.findByIdAndRemove(userId).exec()
                .then((user) => {
                    res.sendStatus(200);

                    return mongo.spaceIds(userId)
                        .then((spaceIds) => Promise.all([
                            mongo.Cluster.remove({space: {$in: spaceIds}}).exec(),
                            mongo.Cache.remove({space: {$in: spaceIds}}).exec(),
                            mongo.DomainModel.remove({space: {$in: spaceIds}}).exec(),
                            mongo.Igfs.remove({space: {$in: spaceIds}}).exec(),
                            mongo.Notebook.remove({space: {$in: spaceIds}}).exec(),
                            mongo.Space.remove({owner: userId}).exec()
                        ]))
                        .then(() => user)
                        .catch((err) => console.error(`Failed to cleanup spaces [user=${user.username}, err=${err}`));
                })
                .then((user) =>
                    mail.send(user, 'Your account was deleted',
                        `Hello ${user.firstName} ${user.lastName}!<br><br>` +
                        `You are receiving this email because your account for <a href="http://${req.headers.host}">${settings.smtp.username}</a> was removed.`,
                        'Account was removed, but failed to send email notification to user!')
                )
                .catch((err) => mongo.handleError(res, err));
        });

        // Save user.
        router.post('/save', (req, res) => {
            const params = req.body;

            mongo.Account.findByIdAndUpdate(params.userId, {admin: params.adminFlag}).exec()
                .then(() => res.sendStatus(200))
                .catch((err) => mongo.handleError(res, err));
        });

        // Become user.
        router.get('/become', (req, res) => {
            mongo.Account.findById(req.query.viewedUserId).exec()
                .then((viewedUser) => {
                    req.session.viewedUser = viewedUser;

                    res.sendStatus(200);
                })
                .catch(() => res.sendStatus(404));
        });

        // Revert to your identity.
        router.get('/revert/identity', (req, res) => {
            req.session.viewedUser = null;

            return res.sendStatus(200);
        });

        factoryResolve(router);
    });
};

