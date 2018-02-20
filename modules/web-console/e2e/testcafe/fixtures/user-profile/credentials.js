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

const { Selector } = require('testcafe');
const { removeData, insertTestUser } = require('../../envtools');
const { signIn } = require('../../roles');

fixture('Checking user credentials change')
    .page `${process.env.APP_URL || 'http://localhost:9001/'}settings/profile`
    .beforeEach(async(t) => {
        await t.setNativeDialogHandler(() => true);
        await removeData();
        await insertTestUser();
        await signIn(t);

        await t.navigateTo(`${process.env.APP_URL || 'http://localhost:9001/'}settings/profile`);
    })
    .after(async() => {
        await removeData();
    });

test('Testing secure token change', async(t) => {
    await t.click(Selector('header').withAttribute('ng-click', '$ctrl.toggleToken()'));

    const currentToken = await Selector('#current-security-token').innerText;

    await t
        .click(Selector('i').withAttribute('ng-click', '$ctrl.generateToken()'))
        .expect(Selector('p').withText('Are you sure you want to change security token?').exists)
        .ok()
        .click('#confirm-btn-ok', {timeout: 5000});

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