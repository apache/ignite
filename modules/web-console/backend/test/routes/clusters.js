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

let agentFactory;
let db;

suite('routes.clusters', () => {
    suiteSetup(() => {
        return Promise.all([injector('agentFactory'), injector('dbHelper')])
            .then(([_agent, _db]) => {
                agentFactory = _agent;
                db = _db;
            });
    });

    setup(() => {
        return db.init();
    });

    test('Save cluster model', (done) => {
        const cluster = Object.assign({}, db.mocks.clusters[0], {name: 'newClusterName'});

        agentFactory.authAgent(db.mocks.accounts[0])
            .then((agent) => {
                agent.put('/api/v1/configuration/clusters')
                    .send({cluster})
                    .expect(200)
                    .expect((res) => {
                        assert.isNotNull(res.body);
                        assert.equal(res.body.rowsAffected, 1);
                    })
                    .end(done);
            })
            .catch(done);
    });

    test('Remove cluster model', (done) => {
        agentFactory.authAgent(db.mocks.accounts[0])
            .then((agent) => {
                agent.post('/api/v1/configuration/clusters/remove')
                    .send({_id: db.mocks.clusters[0]._id})
                    .expect(200)
                    .expect((res) => {
                        assert.isNotNull(res.body);
                        assert.equal(res.body.rowsAffected, 1);
                    })
                    .end(done);
            })
            .catch(done);
    });

    test('Remove all clusters', (done) => {
        agentFactory.authAgent(db.mocks.accounts[0])
            .then((agent) => {
                agent.post('/api/v1/configuration/clusters/remove/all')
                    .expect(200)
                    .expect((res) => {
                        assert.isNotNull(res.body);
                        assert.equal(res.body.rowsAffected, db.mocks.clusters.length);
                    })
                    .end(done);
            })
            .catch(done);
    });
});
