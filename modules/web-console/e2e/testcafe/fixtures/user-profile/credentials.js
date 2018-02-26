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
import { dropTestDB, insertTestUser, resolveUrl } from '../../envtools';
import { createRegularUser } from '../../roles';

const regularUser = createRegularUser();

fixture('Checking user credentials change')
    .before(async() => {
        await dropTestDB();
        await insertTestUser();
    })
    .beforeEach(async(t) => {
        await t.useRole(regularUser);
        await t.navigateTo(resolveUrl('/settings/profile'));
    })
    .after(async() => {
        await dropTestDB();
    });

test('Testing secure token change', async(t) => {
    await t.click(Selector('header').withAttribute('ng-click', '$ctrl.toggleToken()'));

    const currentToken = await Selector('#current-security-token').innerText;

    await t
        .click(Selector('i').withAttribute('ng-click', '$ctrl.generateToken()'))
        .expect(Selector('p').withText('Are you sure you want to change security token?').exists)
        .ok()
        .click('#confirm-btn-ok');

    await t
        .expect(await Selector('#current-security-token').innerText)
        .notEql(currentToken);
});

test('Testing password change', async(t) => {
    await t.click(Selector('header').withAttribute('ng-click', '$ctrl.togglePassword()'));

    await t
        .typeText('#passwordInput', 'newPass')
        .typeText('#passwordConfirmInput', 'newPass')
        .click(Selector('button').withText('Save Changes'));

    await t
        .expect(Selector('span').withText('Profile saved.').exists)
        .ok();
});
