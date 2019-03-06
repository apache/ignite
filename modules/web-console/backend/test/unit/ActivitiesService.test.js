/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

const assert = require('chai').assert;
const injector = require('../injector');
const testAccounts = require('../data/accounts.json');

let activitiesService;
let mongo;
let db;

const owner = testAccounts[0]._id;
const group = 'test';
const action1 = '/test/activity1';
const action2 = '/test/activity2';

suite('ActivitiesServiceTestsSuite', () => {
    suiteSetup(() => {
        return Promise.all([
            injector('services/activities'),
            injector('mongo'),
            injector('dbHelper')
        ])
            .then(([_activitiesService, _mongo, _db]) => {
                mongo = _mongo;
                activitiesService = _activitiesService;
                db = _db;
            });
    });

    setup(() => db.init());

    test('Activities creation and update', (done) => {
        activitiesService.merge(owner, { group, action: action1 })
            .then((activity) => {
                assert.isNotNull(activity);
                assert.equal(activity.amount, 1);

                return mongo.Activities.findById(activity._id);
            })
            .then((activityDoc) => {
                assert.isNotNull(activityDoc);
                assert.equal(activityDoc.amount, 1);
            })
            .then(() => activitiesService.merge(owner, { group, action: action1 }))
            .then((activity) => {
                assert.isNotNull(activity);
                assert.equal(activity.amount, 2);

                return mongo.Activities.findById(activity._id);
            })
            .then((activityDoc) => {
                assert.isNotNull(activityDoc);
                assert.equal(activityDoc.amount, 2);
            })
            .then(done)
            .catch(done);
    });

    test('Activities total and detail information', (done) => {
        const startDate = new Date();

        startDate.setDate(1);
        startDate.setHours(0, 0, 0, 0);

        const endDate = new Date(startDate);
        endDate.setMonth(endDate.getMonth() + 1);

        Promise.all([
            activitiesService.merge(owner, {group, action: action1}),
            activitiesService.merge(owner, {group, action: action2})
        ])
            .then(() => activitiesService.total(owner, {startDate, endDate}))
            .then((activities) =>
                assert.equal(activities[owner].test, 2)
            )
            .then(() => activitiesService.detail(owner, {startDate, endDate}))
            .then((activities) =>
                assert.deepEqual(activities[owner], {
                    '/test/activity2': 1, '/test/activity1': 1
                })
            )
            .then(done)
            .catch(done);
    });

    test('Activities periods', (done) => {
        const startDate = new Date();

        startDate.setDate(1);
        startDate.setHours(0, 0, 0, 0);

        const nextMonth = (baseDate) => {
            const date = new Date(baseDate);

            date.setMonth(date.getMonth() + 1);

            return date;
        };

        const borderDate = nextMonth(startDate);
        const endDate = nextMonth(borderDate);

        activitiesService.merge(owner, { group, action: action1 })
            .then(() => activitiesService.merge(owner, { group, action: action1 }, borderDate))
            .then(() => activitiesService.total({ startDate, endDate: borderDate }))
            .then((activities) =>
                assert.equal(activities[owner].test, 1)
            )
            .then(() => activitiesService.total({ startDate: borderDate, endDate }))
            .then((activities) =>
                assert.equal(activities[owner].test, 1)
            )
            .then(done)
            .catch(done);
    });
});
