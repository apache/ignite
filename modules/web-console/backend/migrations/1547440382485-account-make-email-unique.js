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

const _ = require('lodash');

const log = require('./migration-utils').log;

function deduplicateAccounts(model) {
    const accountsModel = model('Account');
    const spaceModel = model('Space');

    return accountsModel.aggregate([
        {$group: {_id: '$email', count: {$sum: 1}}},
        {$match: {count: {$gt: 1}}}
    ]).exec()
        .then((accounts) => _.map(accounts, '_id'))
        .then((emails) => Promise.all(
            _.map(emails, (email) => accountsModel.find({email}, {_id: 1, email: 1, lastActivity: 1, lastLogin: 1}).lean().exec())
        ))
        .then((promises) => {
            const duplicates = _.flatMap(promises, (accounts) => _.sortBy(accounts, [(a) => a.lastActivity || '', 'lastLogin']).slice(0, -1));

            if (_.isEmpty(duplicates))
                log('Duplicates not found!');
            else {
                log(`Duplicates found: ${_.size(duplicates)}`);

                _.forEach(duplicates, (dup) => log(`  ID: ${dup._id}, e-mail: ${dup.email}`));
            }

            return _.map(duplicates, '_id');
        })
        .then((accountIds) => {
            if (_.isEmpty(accountIds))
                return Promise.resolve();

            return spaceModel.find({owner: {$in: accountIds}}, {_id: 1}).lean().exec()
                .then((spaces) => _.map(spaces, '_id'))
                .then((spaceIds) =>
                    Promise.all([
                        model('Cluster').deleteMany({space: {$in: spaceIds}}).exec(),
                        model('Cache').deleteMany({space: {$in: spaceIds}}).exec(),
                        model('DomainModel').deleteMany({space: {$in: spaceIds}}).exec(),
                        model('Igfs').deleteMany({space: {$in: spaceIds}}).exec(),
                        model('Notebook').deleteMany({space: {$in: spaceIds}}).exec(),
                        model('Activities').deleteMany({owner: accountIds}).exec(),
                        model('Notifications').deleteMany({owner: accountIds}).exec(),
                        spaceModel.deleteMany({owner: accountIds}).exec(),
                        accountsModel.deleteMany({_id: accountIds}).exec()
                    ])
                )
                .then(() => {
                    const conditions = _.map(accountIds, (accountId) => ({session: {$regex: `"${accountId}"`}}));

                    return accountsModel.db.collection('sessions').deleteMany({$or: conditions});
                });
        });
}

exports.up = function up(done) {
    deduplicateAccounts((name) => this(name))
        .then(() => this('Account').collection.createIndex({email: 1}, {unique: true, background: false}))
        .then(() => done())
        .catch(done);
};

exports.down = function down(done) {
    log('Account migration can not be reverted');

    done();
};
