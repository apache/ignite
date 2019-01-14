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

function deduplicate(accountsModel, spaceModel, activitiesModel) {
    return accountsModel.distinct('email')
        .lean()
        .exec()
        .then((emails) => {
            const accountsToRemove = [];

            return _.reduce(emails, (start, email) => start.then(() => {
                return accountsModel.find({email}).count()
                    .then((cnt) => {
                        if (cnt > 1) {
                            return accountsModel.find({email})
                                .lean()
                                .exec()
                                .then((accounts) => {
                                    // Sort by last activity.
                                    const sorted = _.sortBy(accounts, [(o) => { return o.lastActivity; }]);

                                    // And keep most recent.
                                    sorted.splice(-1, 1);

                                    // All other accounts will be deleted.
                                    accountsToRemove.push(...sorted);

                                    return Promise.resolve();
                                });
                        }

                        return Promise.resolve();
                    });
            }), Promise.resolve())
                .then(() => {
                    if (_.isEmpty(accountsToRemove))
                        console.log('Duplicate accounts not found');
                    else {
                        console.log(`Following accounts will be removed as duplicates: ${_.size(accountsToRemove)}`);
                        _.forEach(accountsToRemove, (acc) => console.log(`  ${acc._id} ${acc.email}`));

                        const _ids = _.map(accountsToRemove, (acc) => acc._id);

                        return accountsModel.remove({_id: {$in: _ids}})
                            .then(() => spaceModel.remove({owner: {$in: _ids}}))
                            .then(() => activitiesModel.remove({owner: {$in: _ids}}));
                    }
                });
        });
}

function createIndex(accountsModel) {
    return accountsModel.collection.createIndex({email: 1}, {unique: true, background: false});
}

exports.up = function up(done) {
    process.on('unhandledRejection', function(reason, p) {
        console.log('Unhandled rejection at:', p, 'reason:', reason);
    });

    const accountsModel = this('Account');
    const spaceModel = this('Space');
    const activitiesModel = this('Activities');

    Promise.resolve()
        .then(() => deduplicate(accountsModel, spaceModel, activitiesModel))
        .then(() => createIndex(accountsModel))
        .then(() => done())
        .catch(done);
};

exports.down = function down(done) {
    log('Model migration can not be reverted');

    done();
};
