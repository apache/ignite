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

import {dropTestDB, resolveUrl, insertTestUser} from '../../envtools';
import {PageSignIn} from '../../page-models/PageSignIn';
import {errorNotification} from '../../components/notifications';
import {userMenu} from '../../components/userMenu';

const page = new PageSignIn();

fixture('Signup')
    .page(resolveUrl('/signin'))
    .before(async() => {
        await dropTestDB();
        await insertTestUser();
    })
    .after(async() => {
        await dropTestDB();
    });

test('Local validation', async(t) => {
    const isButtonDisabled = page.signupButton.hasAttribute('disabled');
    await t.expect(isButtonDisabled).ok('Button disabled by default');
    await page.fillSignupForm({
        email: 'foobar',
        password: '1',
        passwordConfirm: '2',
        firstName: '  ',
        lastName: 'Doe',
        company: 'FooBar',
        country: 'Brazil'
    });
    await t
        .expect(page.signup.email.getError('email').exists).ok()
        .expect(page.signup.passwordConfirm.getError('mismatch').exists).ok()
        .expect(page.signup.firstName.getError('required').exists).ok()
        .expect(isButtonDisabled).ok('Button disabled with invalid fields');
    await page.fillSignupForm({
        email: 'foobar@bar.baz',
        password: '1',
        passwordConfirm: '1',
        firstName: 'John',
        lastName: 'Doe',
        company: 'FooBar',
        country: 'Brazil'
    });
    await t.expect(isButtonDisabled).notOk('Button enabled with valid fields');
});
test('Server validation', async(t) => {
    await page.fillSignupForm({
        email: 'a@a',
        password: '1',
        passwordConfirm: '1',
        firstName: 'John',
        lastName: 'Doe',
        company: 'FooBar',
        country: 'Brazil'
    });
    await t
        .click(page.signupButton)
        .expect(errorNotification.withText('A user with the given username is already registered').exists).ok('Shows global error')
        .expect(page.signup.email.getError('server').exists).ok('Marks email input as server-invalid');
});
test('Successful signup', async(t) => {
    await page.fillSignupForm({
        email: 'test@example.com',
        password: '1',
        passwordConfirm: '1',
        firstName: 'John',
        lastName: 'Doe',
        company: 'FooBar',
        country: 'Brazil'
    });
    await t
        .click(page.signupButton)
        .expect(userMenu.button.textContent).contains('John Doe', 'User gets logged in under chosen full name');
});
