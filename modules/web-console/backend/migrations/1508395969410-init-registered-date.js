/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const _ = require('lodash');

exports.up = function up(done) {
    const accountsModel = this('Account');

    accountsModel.find({}).lean().exec()
        .then((accounts) => _.reduce(accounts, (start, account) => start
            .then(() => accountsModel.updateOne({_id: account._id}, {$set: {registered: account.lastLogin}}).exec()), Promise.resolve()))
        .then(() => done())
        .catch(done);
};

exports.down = function down(done) {
    this('Account').updateMany({}, {$unset: {registered: 1}}).exec()
        .then(() => done())
        .catch(done);
};
