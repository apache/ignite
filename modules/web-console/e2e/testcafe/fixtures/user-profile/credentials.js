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
