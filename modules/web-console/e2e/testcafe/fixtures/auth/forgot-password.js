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

import { dropTestDB, resolveUrl, insertTestUser } from '../../environment/envtools';
import {errorNotification} from '../../components/notifications';
import {pageForgotPassword as page} from '../../page-models/pageForgotPassword';

fixture('Password reset')
    .page(resolveUrl('/forgot-password'))
    .before(async() => {
        await dropTestDB();
        await insertTestUser();
    })
    .after(async() => {
        await dropTestDB();
    });

test('Incorrect email', async(t) => {
    await t
        .typeText(page.email.control, 'aa')
        .expect(page.email.getError('email').exists).ok('Marks field as invalid');
});

test('Unknown email', async(t) => {
    await t
        .typeText(page.email.control, 'nonexisting@example.com', {replace: true})
        .click(page.remindPasswordButton)
        .expect(errorNotification.withText('Account with email does not exists: nonexisting@example.com').exists).ok('Shows global error notification')
        .expect(page.email.getError('server').exists).ok('Marks input as server-invalid');
});

// TODO: IGNITE-8028 Implement this test as unit test.
test.skip('Successful reset', async(t) => {
    await t
        .typeText(page.email.control, 'a@example.com', {replace: true})
        .click(page.remindPasswordButton)
        .expect(page.email.getError('server').exists).notOk('No errors happen');
});
