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

import { dropTestDB, insertTestUser, resolveUrl } from '../../environment/envtools';
import { createRegularUser } from '../../roles';
import {pageProfile} from '../../page-models/pageProfile';
import {confirmation} from '../../components/confirmation';
import {successNotification} from '../../components/notifications';

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
    await t.click(pageProfile.securityToken.panel.heading);

    const currentToken = await pageProfile.securityToken.value.control.value;

    await t
        .click(pageProfile.securityToken.generateTokenButton)
        .expect(confirmation.body.innerText).contains(
`Are you sure you want to change security token?
If you change the token you will need to restart the agent.`
        )
        .click(confirmation.confirmButton)
        .expect(pageProfile.securityToken.value.control.value).notEql(currentToken);
});

test('Testing password change', async(t) => {
    const pass = 'newPass';

    await t
        .click(pageProfile.password.panel.heading)
        .typeText(pageProfile.password.newPassword.control, pass)
        .typeText(pageProfile.password.confirmPassword.control, pass)
        .click(pageProfile.saveChangesButton)
        .expect(successNotification.withText('Profile saved.').exists).ok();
});
