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

// eslint-disable-next-line
import {default as AuthService} from 'app/modules/user/Auth.service';

export default class {
    /** @type {ng.IFormController} */
    form_signup;
    /** @type {ng.IFormController} */
    form_signin;
    /** @type {ng.IFormController} */
    form_forgot;

    static $inject = ['IgniteFocus', 'IgniteCountries', 'Auth', 'IgniteMessages'];

    /**
     * @param {AuthService} Auth
     */
    constructor(Focus, Countries, Auth, IgniteMessages) {
        this.Auth = Auth;
        this.IgniteMessages = IgniteMessages;
        this.countries = Countries.getAll();

        Focus.move('user_email');
    }
    $onInit() {
        const data = this.data = {
            signup: {
                email: null,
                password: null,
                firstName: null,
                lastName: null,
                company: null,
                country: null
            },
            signin: {
                /** @type {string} */
                email: null,
                /** @type {string} */
                password: null
            },
            remindPassword: {
                /** 
                 * Explicitly mirrors signin email so user won't have to type it twice
                 * @type {string}
                 */
                get email() {
                    return data.signin.email;
                },
                set email(value) {
                    data.signin.email = value;
                }
            }
        };
        /** @type {('signin'|'remindPassword')} */
        this.activeForm = 'signin';
        /** @type {{signup: string, signin: string, remindPassword: string}} */
        this.serverErrors = {
            signup: null,
            signin: null,
            remindPassword: null
        };
    }
    get isSigninOpened() {
        return this.activeForm === 'signin';
    }
    get isRemindPasswordOpened() {
        return this.activeForm === 'remindPassword';
    }
    /** Toggles between signin and remind password forms */
    toggleActiveForm() {
        this.activeForm = this.activeForm === 'signin' ? 'remindPassword' : 'signin';
    }
    /** @param {ng.IFormController} form */
    canSubmitForm(form) {
        return form.$error.server ? true : !form.$invalid;
    }
    $postLink() {
        this.form_signup.signupEmail.$validators.server = () => !this.serverErrors.signup;
        this.form_signin.signinEmail.$validators.server = () => !this.serverErrors.signin;
        this.form_signin.signinPassword.$validators.server = () => !this.serverErrors.signin;
        this.form_forgot.forgotEmail.$validators.server = () => !this.serverErrors.remindPassword;
    }
    signup() {
        return this.Auth.signnup(this.data.signup).catch((res) => {
            this.IgniteMessages.showError(null, res.data);
            this.serverErrors.signup = res.data;
            this.form_signup.signupEmail.$validate();
        });
    }
    signin() {
        return this.Auth.signin(this.data.signin.email, this.data.signin.password).catch((res) => {
            this.IgniteMessages.showError(null, res.data);
            this.serverErrors.signin = res.data;
            this.form_signin.signinEmail.$validate();
            this.form_signin.signinPassword.$validate();
        });
    }
    remindPassword() {
        return this.Auth.remindPassword(this.data.remindPassword.email).catch((res) => {
            this.IgniteMessages.showError(null, res.data);
            this.serverErrors.remindPassword = res.data;
            this.form_forgot.forgotEmail.$validate();
        });
    }
}
