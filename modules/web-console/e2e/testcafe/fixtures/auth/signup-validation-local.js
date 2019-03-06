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
