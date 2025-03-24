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

import { Selector } from 'testcafe';
import { dropTestDB, insertTestUser, resolveUrl } from '../environment/envtools';
import { createRegularUser } from '../roles';
import { queriesNavButton, configureNavButton } from '../components/topNavigation';

const regularUser = createRegularUser();

fixture('Checking Ingite main menu')
    .before(async() => {
        await dropTestDB();
        await insertTestUser();
    })
    .beforeEach(async(t) => {
        await t.useRole(regularUser);
        await t.navigateTo(resolveUrl('/'));
    })
    .after(async() => {
        await dropTestDB();
    });

test('Ignite main menu smoke test', async(t) => {
    await t
        .click(configureNavButton)
        .expect(Selector('title').innerText)
        .eql('Configuration – Apache Ignite Web Console');

    await t
        .click(queriesNavButton)
        .expect(Selector('title').innerText)
        .eql('Notebooks – Apache Ignite Web Console');
});
