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
const { removeData, insertTestUser } = require('../envtools');
const { signIn } = require('../roles');

fixture('Checking admin panel')
    .page `${process.env.APP_URL || 'http://localhost:9001/'}settings/admin`
    .beforeEach(async(t) => {
        await t.setNativeDialogHandler(() => true);
        await removeData();
        await insertTestUser();
        await signIn(t);

        await t.navigateTo(`${process.env.APP_URL || 'http://localhost:9001/'}settings/admin`);
    })
    .after(async() => {
        await removeData();
    });

test('Testing setting notifications', async(t) => {
    await t.click(Selector('button').withAttribute('ng-click', 'ctrl.changeUserNotifications()'));

    await t
        .expect(Selector('h4').withText(/.*Set user notifications.*/).exists)
        .ok()
        .click('.ace_content')
        .pressKey('t e s t space m e s s a g e')
        .click('#btn-submit');

    await t
        .expect(Selector('div').withText('test message').exists)
        .ok();

    await t.click(Selector('button').withAttribute('ng-click', 'ctrl.changeUserNotifications()'));

    await t
        .click('.ace_content')
        .pressKey('ctrl+a delete')
        .click('#btn-submit');

    await t
        .expect(Selector('div').withText('test message').exists)
        .notOk();
});
