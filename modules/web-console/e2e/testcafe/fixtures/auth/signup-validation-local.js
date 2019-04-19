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

import {resolveUrl} from '../../environment/envtools';
import {pageSignup as page} from '../../page-models/pageSignup';

fixture('Signup validation local').page(resolveUrl('/signup'));

test('Most fields have validation', async(t) => {
    await page.fillSignupForm({
        email: 'foobar',
        password: '1',
        passwordConfirm: '2',
        firstName: '  ',
        lastName: '   ',
        company: '   ',
        country: 'Brazil'
    });

    await t
        .expect(page.email.getError('email').exists).ok()
        .expect(page.passwordConfirm.getError('mismatch').exists).ok()
        .expect(page.firstName.getError('required').exists).ok()
        .expect(page.lastName.getError('required').exists).ok()
        .expect(page.company.getError('required').exists).ok();
});

test('Errors on submit', async(t) => {
    await t
        .typeText(page.email.control, 'email@example.com')
        .click(page.signupButton)
        .expect(page.password.control.focused).ok()
        .expect(page.password.getError('required').exists).ok()
        .typeText(page.password.control, 'Foo')
        .click(page.signupButton)
        .expect(page.passwordConfirm.control.focused).ok()
        .expect(page.passwordConfirm.getError('required').exists).ok();
});
