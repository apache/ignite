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

exports.up = function up(done) {
    const accountsModel = this('Account');

    accountsModel.find({}).lean().exec()
        .then((accounts) => _.reduce(accounts, (start, account) => start
            .then(() => accountsModel.update({_id: account._id}, {$set: {registered: account.lastLogin}}).exec()), Promise.resolve()))
        .then(() => done())
        .catch(done);
};

exports.down = function down(done) {
    this('Account').update({}, {$unset: {registered: 1}}, {multi: true}).exec()
        .then(() => done())
        .catch(done);
};
