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

const _ = require('lodash');

// Fire me up!

module.exports = {
    implements: 'services/users',
    inject: ['errors', 'settings', 'mongo', 'services/spaces', 'services/mails', 'services/activities', 'services/utils', 'agents-handler']
};

/**
 * @param mongo
 * @param errors
 * @param settings
 * @param {SpacesService} spacesService
 * @param {MailsService} mailsService
 * @param {ActivitiesService} activitiesService
 * @param {UtilsService} utilsService
 * @param {AgentsHandler} agentHnd
 * @returns {UsersService}
 */
module.exports.factory = (errors, settings, mongo, spacesService, mailsService, activitiesService, utilsService, agentHnd) => {
    class UsersService {
        /**
         * Save profile information.
         *
         * @param {String} host - The host.
         * @param {Object} user - The user.
         * @param {Object} createdByAdmin - Whether user created by admin.
         * @returns {Promise.<mongo.ObjectId>} that resolves account id of merge operation.
         */
        static create(host, user, createdByAdmin) {
            return mongo.Account.count().exec()
                .then((cnt) => {
                    user.admin = cnt === 0;
                    user.registered = new Date();
                    user.token = utilsService.randomString(settings.tokenLength);
                    user.resetPasswordToken = utilsService.randomString(settings.tokenLength);
                    user.activated = false;

                    if (settings.activation.enabled) {
                        user.activationToken = utilsService.randomString(settings.tokenLength);
                        user.activationSentAt = new Date();
                    }

                    if (settings.server.disableSignup && !user.admin && !createdByAdmin)
                        throw new errors.ServerErrorException('Sign-up is not allowed. Ask your Web Console administrator to create account for you.');

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
                    return mongo.Space.create({name: 'Personal space', owner: registered._id})
                        .then(() => registered);
                })
                .then((registered) => {
                    if (settings.activation.enabled) {
                        mailsService.sendActivationLink(host, registered);

                        if (createdByAdmin)
                            return registered;

                        throw new errors.MissingConfirmRegistrationException(registered.email);
                    }

                    mailsService.sendWelcomeLetter(host, registered, createdByAdmin);

                    return registered;
                });
        }

        /**
         * Save user.
         *
         * @param userId User ID.
         * @param {Object} changed Changed user.
         * @returns {Promise.<mongo.ObjectId>} that resolves account id of merge operation.
         */
        static save(userId, changed) {
            delete changed.admin;
            delete changed.activated;
            delete changed.activationSentAt;
            delete changed.activationToken;

            return mongo.Account.findById(userId).exec()
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
                        agentHnd.onTokenReset(user);

                    _.extend(user, changed);

                    return user.save();
                });
        }

        /**
         * Get list of user accounts and summary information.
         *
         * @returns {mongo.Account[]} - returns all accounts with counters object
         */
        static list(params) {
            return Promise.all([
                Promise.all([
                    mongo.Account.aggregate([
                        {$lookup: {from: 'spaces', localField: '_id', foreignField: 'owner', as: 'spaces'}},
                        {$project: {
                            _id: 1,
                            firstName: 1,
                            lastName: 1,
                            admin: 1,
                            email: 1,
                            company: 1,
                            country: 1,
                            lastLogin: 1,
                            lastActivity: 1,
                            activated: 1,
                            spaces: {
                                $filter: {
                                    input: '$spaces',
                                    as: 'space',
                                    cond: {$eq: ['$$space.demo', false]}
                                }
                            }
                        }},
                        { $sort: {firstName: 1, lastName: 1}}
                    ]).exec(),
                    mongo.Cluster.aggregate([{$group: {_id: '$space', count: { $sum: 1 }}}]).exec(),
                    mongo.Cache.aggregate([{$group: {_id: '$space', count: { $sum: 1 }}}]).exec(),
                    mongo.DomainModel.aggregate([{$group: {_id: '$space', count: { $sum: 1 }}}]).exec(),
                    mongo.Igfs.aggregate([{$group: {_id: '$space', count: { $sum: 1 }}}]).exec()
                ]).then(([users, clusters, caches, models, igfs]) => {
                    const clustersMap = _.mapValues(_.keyBy(clusters, '_id'), 'count');
                    const cachesMap = _.mapValues(_.keyBy(caches, '_id'), 'count');
                    const modelsMap = _.mapValues(_.keyBy(models, '_id'), 'count');
                    const igfsMap = _.mapValues(_.keyBy(igfs, '_id'), 'count');

                    _.forEach(users, (user) => {
                        const counters = user.counters = {};

                        counters.clusters = _.sumBy(user.spaces, ({_id}) => clustersMap[_id]) || 0;
                        counters.caches = _.sumBy(user.spaces, ({_id}) => cachesMap[_id]) || 0;
                        counters.models = _.sumBy(user.spaces, ({_id}) => modelsMap[_id]) || 0;
                        counters.igfs = _.sumBy(user.spaces, ({_id}) => igfsMap[_id]) || 0;

                        delete user.spaces;
                    });

                    return users;
                }),
                activitiesService.total(params),
                activitiesService.detail(params)
            ])
                .then(([users, activitiesTotal, activitiesDetail]) => {
                    _.forEach(users, (user) => {
                        user.activitiesTotal = activitiesTotal[user._id];
                        user.activitiesDetail = activitiesDetail[user._id];
                    });

                    return users;
                });
        }

        /**
         * Remove account.
         *
         * @param {String} host.
         * @param {mongo.ObjectId|String} userId - The account id for remove.
         * @returns {Promise.<{rowsAffected}>} - The number of affected rows.
         */
        static remove(host, userId) {
            return mongo.Account.findByIdAndRemove(userId).exec()
                .then((user) => {
                    return spacesService.spaceIds(userId)
                        .then((spaceIds) => Promise.all([
                            mongo.Cluster.deleteMany({space: {$in: spaceIds}}).exec(),
                            mongo.Cache.deleteMany({space: {$in: spaceIds}}).exec(),
                            mongo.DomainModel.deleteMany({space: {$in: spaceIds}}).exec(),
                            mongo.Igfs.deleteMany({space: {$in: spaceIds}}).exec(),
                            mongo.Notebook.deleteMany({space: {$in: spaceIds}}).exec(),
                            mongo.Space.deleteOne({owner: userId}).exec()
                        ]))
                        .catch((err) => console.error(`Failed to cleanup spaces [user=${user.username}, err=${err}`))
                        .then(() => user);
                })
                .then((user) => mailsService.sendAccountDeleted(host, user));
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
