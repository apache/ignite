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
import { AngularJSSelector } from 'testcafe-angular-selectors';
import { dropTestDB, insertTestUser, resolveUrl } from '../environment/envtools';
import { createRegularUser } from '../roles';

const regularUser = createRegularUser();

fixture('Checking admin panel')
    .before(async() => {
        await dropTestDB();
        await insertTestUser();
    })
    .beforeEach(async(t) => {
        await t.useRole(regularUser);
        await t.navigateTo(resolveUrl('/settings/admin'));
    })
    .after(async() => {
        await dropTestDB();
    });

const setNotificationsButton = Selector('button').withText('Set user notifications');

test('Testing setting notifications', async(t) => {
    await t.click(setNotificationsButton);

    await t
        .expect(Selector('h4').withText(/.*Set user notifications.*/).exists)
        .ok()
        .click('.ace_content')
        .pressKey('ctrl+a delete')
        .pressKey('t e s t space m e s s a g e')
        .click(AngularJSSelector.byModel('$ctrl.isShown'))
        .click('#btn-submit');

    await t
        .expect(Selector('.wch-notification').innerText)
        .eql('test message');

    await t.click(setNotificationsButton);

    await t
        .click('.ace_content')
        .pressKey('ctrl+a delete')
        .click(AngularJSSelector.byModel('$ctrl.isShown'))
        .click('#btn-submit');

    await t
        .expect(Selector('.wch-notification', { visibilityCheck: false } ).visible)
        .notOk();
});
