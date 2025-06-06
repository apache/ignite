/*
 * Copyright 2019 Ignite Systems, Inc. and Contributors.
 *
 * Licensed under the Ignite Community Edition License (the "License");
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

import { dropTestDB, insertTestUser, resolveUrl } from '../../environment/envtools';
import { createRegularUser } from '../../roles';
import * as admin from '../../page-models/pageAdminListOfRegisteredUsers';
import {pageProfile as profile} from '../../page-models/pageProfile';
import * as notifications from '../../components/permanentNotifications';
import {userMenu} from '../../components/userMenu';

const regularUser = createRegularUser();

fixture('Assumed identity')
    .beforeEach(async(t) => {
        await dropTestDB();
        await insertTestUser();
        await insertTestUser({
            email: 'foo@example.com',
            password: '1',
            passwordConfirm: '1',
            firstName: 'User',
            lastName: 'Name',
            country: 'Brazil',
            company: 'Acme Inc.',
            industry: 'Banking'
        });
        await t.useRole(regularUser);
    })
    .afterEach(async() => {
        await dropTestDB();
    });

test('Become user', async(t) => {
    await t.navigateTo(resolveUrl('/settings/admin'));
    await t.click(admin.userNameCell.withText('User Name'));
    await admin.usersTable.performAction('Become this user');
    await t
        .hover(userMenu.button)
        // See https://ggsystems.atlassian.net/browse/GG-24068
        .expect(userMenu.menuItem.withText('Admin panel').exists).notOk()
        .expect(userMenu.menuItem.withText('Log out').exists).notOk();
    await userMenu.clickOption('Profile');
    await t
        .expect(notifications.assumedUserFullName.innerText).eql('User Name')
        .typeText(profile.firstName.control, '1')
        .click(profile.saveChangesButton)
        .expect(notifications.assumedIdentityNotification.visible).ok()
        .click(notifications.revertIdentityButton)
        .hover(userMenu.button)
        // See https://ggsystems.atlassian.net/browse/GG-24068
        .expect(userMenu.menuItem.withText('Admin panel').count).eql(1);
});
