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

import {Selector, t} from 'testcafe';
import {CustomFormField} from '../components/FormField';

export const pageSignup = {
    email: new CustomFormField({id: 'emailInput'}),
    password: new CustomFormField({id: 'passwordInput'}),
    passwordConfirm: new CustomFormField({id: 'confirmInput'}),
    firstName: new CustomFormField({id: 'firstNameInput'}),
    lastName: new CustomFormField({id: 'lastNameInput'}),
    company: new CustomFormField({id: 'companyInput'}),
    country: new CustomFormField({id: 'countryInput'}),
    signupButton: Selector('button').withText('Sign Up'),
    async fillSignupForm({
        email,
        password,
        passwordConfirm,
        firstName,
        lastName,
        company,
        country
    }) {
        await t
            .typeText(this.email.control, email, {replace: true})
            .typeText(this.password.control, password, {replace: true})
            .typeText(this.passwordConfirm.control, passwordConfirm, {replace: true})
            .typeText(this.firstName.control, firstName, {replace: true})
            .typeText(this.lastName.control, lastName, {replace: true})
            .typeText(this.company.control, company, {replace: true});
        await this.country.selectOption(country);
    }
};
