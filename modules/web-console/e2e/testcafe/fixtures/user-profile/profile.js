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
