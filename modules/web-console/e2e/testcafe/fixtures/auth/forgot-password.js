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

import { dropTestDB, resolveUrl, insertTestUser } from '../../envtools';
import {PageSignIn} from '../../page-models/PageSignin';
import {errorNotification} from '../../components/notifications';

const page = new PageSignIn();

fixture('Password reset')
    .page(resolveUrl('/signin'))
    .before(async() => {
        await dropTestDB();
        await insertTestUser();
    })
    .after(async() => {
        await dropTestDB();
    });

test('Incorrect email', async(t) => {
    await t
        .click(page.showForgotPasswordButton)
        .typeText(page.forgotPassword.email.control, 'aa')
        .expect(page.forgotPassword.email.getError('email').exists).ok('Marks field as invalid')
        .expect(page.remindPasswordButton.getAttribute('disabled')).ok('Disables submit button');
});

test('Unknown email', async(t) => {
    await t
        .click(page.showForgotPasswordButton)
        .typeText(page.forgotPassword.email.control, 'nonexisting@mail.com', {replace: true})
        .click(page.remindPasswordButton)
        .expect(errorNotification.withText('Account with that email address does not exists!').exists).ok('Shows global error notification')
        .expect(page.forgotPassword.email.getError('server').exists).ok('Marks input as server-invalid');
});

// Will fail without a local SMTP server.
test('Successful reset', async(t) => {
    await t
        .click(page.showForgotPasswordButton)
        .typeText(page.forgotPassword.email.control, 'a@a', {replace: true})
        .click(page.remindPasswordButton)
        .expect(page.forgotPassword.email.getError('server').exists).notOk('No errors happen');
});
