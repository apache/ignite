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

import { Selector } from 'testcafe';
import { dropTestDB, insertTestUser, resolveUrl } from '../envtools';
import { createRegularUser } from '../roles';

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

test('Ingite main menu smoke test', async(t) => {
    await t
        .click(Selector('a').withAttribute('ui-sref', 'base.configuration.tabs'))
        .expect(Selector('title').innerText)
        .eql('Basic Configuration – Apache Ignite Web Console');

    await t
        .click(Selector('a').withText('Queries'))
        .expect(Selector('h4').withText('New query notebook').exists)
        .ok()
        .typeText('#create-notebook', 'Test query')
        .click('#copy-btn-confirm');

    await t
        .expect(Selector('span').withText('Connection to Ignite Web Agent is not established').exists)
        .ok();
});
