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
