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
import { resolveUrl } from '../envtools';
import {AngularJSSelector} from 'testcafe-angular-selectors';
import {CustomFormField} from '../components/FormField';

export class PageSignIn {
    constructor() {
        this._selector = Selector('page-sign-in');
        this.signin = {
            email: new CustomFormField({model: '$ctrl.data.signin.email'}),
            password: new CustomFormField({model: '$ctrl.data.signin.password'})
        };
        this.forgotPassword = {
            email: new CustomFormField({model: '$ctrl.data.remindPassword.email'})
        };
        this.signup = {
            email: new CustomFormField({model: '$ctrl.data.signup.email'}),
            password: new CustomFormField({model: '$ctrl.data.signup.password'}),
            passwordConfirm: new CustomFormField({model: 'confirm'}),
            firstName: new CustomFormField({model: '$ctrl.data.signup.firstName'}),
            lastName: new CustomFormField({model: '$ctrl.data.signup.lastName'}),
            company: new CustomFormField({model: '$ctrl.data.signup.company'}),
            country: new CustomFormField({model: '$ctrl.data.signup.country'})
        };
        this.signinButton = Selector('#signin_submit');
        this.signupButton = Selector('#signup_submit');
        this.showForgotPasswordButton = Selector('#forgot_show');
        this.remindPasswordButton = Selector('#forgot_submit');
    }

    async open() {
        await t.navigateTo(resolveUrl('/signin'));
    }

    async login(email, password) {
        return await t
            .typeText(this.signin.email.control, email)
            .typeText(this.signin.password.control, password)
            .click(this.signinButton);
    }

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
            .typeText(this.signup.email.control, email, {replace: true})
            .typeText(this.signup.password.control, password, {replace: true})
            .typeText(this.signup.passwordConfirm.control, passwordConfirm, {replace: true})
            .typeText(this.signup.firstName.control, firstName, {replace: true})
            .typeText(this.signup.lastName.control, lastName, {replace: true})
            .typeText(this.signup.company.control, company, {replace: true});
        await this.signup.country.selectOption(country);
    }
}
