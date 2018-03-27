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
import { dropTestDB, insertTestUser, resolveUrl } from '../../envtools';
import { createRegularUser } from '../../roles';

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
    const newUserData = {
        firstName: {
            selector: '#firstNameInput',
            value: 'Richard'
        },
        lastName: {
            selector: '#lastNameInput',
            value: 'Roe'
        },
        email: {
            selector: '#emailInput',
            value: 'r.roe@mail.com'
        },
        company: {
            selector: '#companyInput',
            value: 'New Company'
        },
        country: {
            selector: '#countryInput',
            value: 'Israel'
        }
    };

    ['firstName', 'lastName', 'email', 'company'].forEach(async(item) => {
        await t
            .click(newUserData[item].selector)
            .pressKey('ctrl+a delete')
            .typeText(newUserData[item].selector, newUserData[item].value);
    });

    await t
        .click(newUserData.country.selector)
        .click(Selector('span').withText(newUserData.country.value))
        .click(Selector('button').withText('Save Changes'));

    await t.navigateTo(resolveUrl('/settings/profile'));

    ['firstName', 'lastName', 'email', 'company'].forEach(async(item) => {
        await t
            .expect(await Selector(newUserData[item].selector).getAttribute('value'))
            .eql(newUserData[item].value);
    });

    await t
        .expect(Selector(newUserData.country.selector).innerText)
        .eql(newUserData.country.value);
});
