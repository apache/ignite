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

const assert = require('chai').assert;
const injector = require('../injector');

const testAccounts = require('../data/accounts.json');

let agentFactory;
let db;

suite('routes.public', () => {
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

    test('Login success', (done) => {
        const user = testAccounts[0];

        agentFactory.guestAgent()
            .then((agent) => {
                agent.post('/api/v1/signin')
                    .send({email: user.email, password: user.password})
                    .expect(200)
                    .expect((res) => {
                        assert.isNotNull(res.headers['set-cookie']);
                        assert.match(res.headers['set-cookie'], /connect\.sid/);
                    })
                    .end(done);
            })
            .catch(done);
    });

    test('Login fail', (done) => {
        const user = testAccounts[0];

        agentFactory.guestAgent()
            .then((agent) => {
                agent.post('/api/v1/signin')
                    .send({email: user.email, password: 'notvalidpassword'})
                    .expect(401)
                    .end(done);
            })
            .catch(done);
    });
});
