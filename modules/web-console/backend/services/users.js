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
    implements: 'services/users',
    inject: ['require(lodash)', 'mongo', 'settings', 'services/spaces', 'services/mails', 'services/activities', 'agent-manager', 'errors']
};

/**
 * @param _
 * @param mongo
 * @param settings
 * @param {SpacesService} spacesService
 * @param {MailsService} mailsService
 * @param {ActivitiesService} activitiesService
 * @param agentMgr
 * @param errors
 * @returns {UsersService}
 */
module.exports.factory = (_, mongo, settings, spacesService, mailsService, activitiesService, agentMgr, errors) => {
    const _randomString = () => {
        const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
        const possibleLen = possible.length;

        let res = '';

        for (let i = 0; i < settings.tokenLength; i++)
            res += possible.charAt(Math.floor(Math.random() * possibleLen));

        return res;
    };

    class UsersService {
        /**
         * Save profile information.
         * @param {String} host - The host
         * @param {Object} user - The user
         * @returns {Promise.<mongo.ObjectId>} that resolves account id of merge operation.
         */
        static create(host, user) {
            return mongo.Account.count().exec()
                .then((cnt) => {
                    user.admin = cnt === 0;

                    user.token = _randomString();

                    return new mongo.Account(user);
                })
                .then((created) => {
                    return new Promise((resolve, reject) => {
                        mongo.Account.register(created, user.password, (err, registered) => {
                            if (err)
                                reject(err);

                            if (!registered)
                                reject(new errors.ServerErrorException('Failed to register user.'));

                            resolve(registered);
                        });
                    });
                })
                .then((registered) => {
                    registered.resetPasswordToken = _randomString();

                    return registered.save()
                        .then(() => mongo.Space.create({name: 'Personal space', owner: registered._id}))
                        .then(() => {
                            mailsService.emailUserSignUp(host, registered)
                                .catch((err) => console.error(err));

                            return registered;
                        });
                });
        }

        /**
         * Save user.
         * @param {Object} changed - The user
         * @returns {Promise.<mongo.ObjectId>} that resolves account id of merge operation.
         */
        static save(changed) {
            return mongo.Account.findById(changed._id).exec()
                .then((user) => {
                    if (!changed.password)
                        return Promise.resolve(user);

                    return new Promise((resolve, reject) => {
                        user.setPassword(changed.password, (err, _user) => {
                            if (err)
                                return reject(err);

                            delete changed.password;

                            resolve(_user);
                        });
                    });
                })
                .then((user) => {
                    if (!changed.email || user.email === changed.email)
                        return Promise.resolve(user);

                    return new Promise((resolve, reject) => {
                        mongo.Account.findOne({email: changed.email}, (err, _user) => {
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
                    if (changed.token && user.token !== changed.token)
                        agentMgr.close(user._id, user.token);

                    _.extend(user, changed);

                    return user.save();
                });
        }

        /**
         * Get list of user accounts and summary information.
         * @returns {mongo.Account[]} - returns all accounts with counters object
         */
        static list(params) {
            return Promise.all([
                mongo.Space.aggregate([
                    {$match: {demo: false}},
                    {$lookup: {from: 'clusters', localField: '_id', foreignField: 'space', as: 'clusters'}},
                    {$lookup: {from: 'caches', localField: '_id', foreignField: 'space', as: 'caches'}},
                    {$lookup: {from: 'domainmodels', localField: '_id', foreignField: 'space', as: 'domainmodels'}},
                    {$lookup: {from: 'igfs', localField: '_id', foreignField: 'space', as: 'igfs'}},
                    {
                        $project: {
                            owner: 1,
                            clusters: {$size: '$clusters'},
                            models: {$size: '$domainmodels'},
                            caches: {$size: '$caches'},
                            igfs: {$size: '$igfs'}
                        }
                    }
                ]).exec(),
                activitiesService.total(params),
                activitiesService.detail(params),
                mongo.Account.find({}).sort('firstName lastName').lean().exec()
            ])
                .then(([counters, activitiesTotal, activitiesDetail, users]) => {
                    const countersMap = _.keyBy(counters, 'owner');

                    _.forEach(users, (user) => {
                        user.counters = _.omit(countersMap[user._id], '_id', 'owner');
                        user.activitiesTotal = activitiesTotal[user._id];
                        user.activitiesDetail = activitiesDetail[user._id];
                    });

                    return users;
                });
        }

        /**
         * Remove account.
         * @param {String} host.
         * @param {mongo.ObjectId|String} userId - The account id for remove.
         * @returns {Promise.<{rowsAffected}>} - The number of affected rows.
         */
        static remove(host, userId) {
            return mongo.Account.findByIdAndRemove(userId).exec()
                .then((user) => {
                    return spacesService.spaceIds(userId)
                        .then((spaceIds) => Promise.all([
                            mongo.Cluster.remove({space: {$in: spaceIds}}).exec(),
                            mongo.Cache.remove({space: {$in: spaceIds}}).exec(),
                            mongo.DomainModel.remove({space: {$in: spaceIds}}).exec(),
                            mongo.Igfs.remove({space: {$in: spaceIds}}).exec(),
                            mongo.Notebook.remove({space: {$in: spaceIds}}).exec(),
                            mongo.Space.remove({owner: userId}).exec()
                        ]))
                        .catch((err) => console.error(`Failed to cleanup spaces [user=${user.username}, err=${err}`))
                        .then(() => user);
                })
                .then((user) => mailsService.emailUserDeletion(host, user).catch((err) => console.error(err)));
        }

        /**
         * Get account information.
         */
        static get(user, viewedUser) {
            if (_.isNil(user))
                return Promise.reject(new errors.AuthFailedException('The user profile service failed the sign in. User profile cannot be loaded.'));

            const becomeUsed = viewedUser && user.admin;

            if (becomeUsed)
                user = _.extend({}, viewedUser, {becomeUsed: true, becameToken: user.token});
            else
                user = user.toJSON();

            return mongo.Space.findOne({owner: user._id, demo: true}).exec()
                .then((demoSpace) => {
                    if (user && demoSpace)
                        user.demoCreated = true;

                    return user;
                });
        }
    }

    return UsersService;
};
