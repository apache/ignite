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
import { dropTestDB, insertTestUser, resolveUrl } from '../../environment/envtools';
import { createRegularUser } from '../../roles';
import {pageProfile} from '../../page-models/pageProfile';

const regularUser = createRegularUser();

fixture('Checking user profile')
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

test('Testing user data change', async(t) => {
    const firstName = 'Richard';
    const lastName = 'Roe';
    const email = 'r.roe@mail.com';
    const company = 'New Company';
    const country = 'Israel';

    await t
        .typeText(pageProfile.firstName.control, firstName, {replace: true})
        .typeText(pageProfile.lastName.control, lastName, {replace: true})
        .typeText(pageProfile.email.control, email, {replace: true})
        .typeText(pageProfile.company.control, company, {replace: true});
    await pageProfile.country.selectOption(country);
    await t.click(pageProfile.saveChangesButton);

    await t
        .navigateTo(resolveUrl('/settings/profile'))
        .expect(pageProfile.firstName.control.value).eql(firstName)
        .expect(pageProfile.lastName.control.value).eql(lastName)
        .expect(pageProfile.email.control.value).eql(email)
        .expect(pageProfile.company.control.value).eql(company)
        .expect(pageProfile.country.control.innerText).eql(country);
});
