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

import {createRegularUser} from '../roles';
import {dropTestDB, insertTestUser, resolveUrl} from '../environment/envtools';
import {connectedClustersBadge} from '../components/connectedClustersBadge';
import {WebSocketHook} from '../mocks/WebSocketHook';
import {agentStat, FAKE_CLUSTERS} from '../mocks/agentTasks';

const user = createRegularUser();

fixture('Connected clusters')
    .beforeEach(async (t) => {
        await dropTestDB();
        await insertTestUser();
        await t.addRequestHooks(t.ctx.ws = new WebSocketHook().use(agentStat(FAKE_CLUSTERS)));
    })
    .afterEach(async (t) => {
        t.ctx.ws.destroy();
        await dropTestDB();
    });

test('Connected clusters badge', async (t) => {
    await t
        .useRole(user)
        .navigateTo(resolveUrl('/settings/profile'))
        .expect(connectedClustersBadge.textContent).eql('My Connected Clusters: 2');
});
