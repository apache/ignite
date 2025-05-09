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

import {dropTestDB, resolveUrl, insertTestUser} from '../../environment/envtools';
import {createRegularUser} from '../../roles';
import {userMenu} from '../../components/userMenu';
import {pageSignin} from '../../page-models/pageSignin';

const user = createRegularUser();

fixture('Logout')
    .before(async() => {
        await dropTestDB();
        await insertTestUser();
    })
    .after(async() => {
        await dropTestDB();
    });

test('Successful logout', async(t) => {
    await t.useRole(user).navigateTo(resolveUrl('/settings/profile'));
    await userMenu.clickOption('Log out');
    await t.expect(pageSignin.selector.exists).ok('Goes to sign in page after logout');
});
