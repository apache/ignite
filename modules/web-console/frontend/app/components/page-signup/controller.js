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

export default class PageSignup {
    /** @type {import('./types').ISignupFormController} */
    form;
    /** @type {import('./types').ISignupData} */
    data = {
        email: null,
        password: null,
        firstName: null,
        lastName: null,
        company: null,
        country: null
    };
    /** @type {string} */
    serverError = null;

    static $inject = ['IgniteCountries', 'Auth', 'IgniteMessages', 'IgniteFormUtils'];

    /**
     * @param {ReturnType<typeof import('app/services/Countries.service').default>} Countries
     * @param {import('app/modules/user/Auth.service').default} Auth
     * @param {ReturnType<typeof import('app/services/Messages.service').default>} IgniteMessages
     * @param {ReturnType<typeof import('app/services/FormUtils.service').default>} IgniteFormUtils
     */
    constructor(Countries, Auth, IgniteMessages, IgniteFormUtils) {
        this.Auth = Auth;
        this.IgniteMessages = IgniteMessages;
        this.countries = Countries.getAll();
        this.IgniteFormUtils = IgniteFormUtils;
    }

    /** @param {import('./types').ISignupFormController} form */
    canSubmitForm(form) {
        return form.$error.server ? true : !form.$invalid;
    }

    $postLink() {
        this.form.email.$validators.server = () => !this.serverError;
    }

    /** @param {string} error */
    setServerError(error) {
        this.serverError = error;
        this.form.email.$validate();
    }

    signup() {
        this.IgniteFormUtils.triggerValidation(this.form);

        this.setServerError(null);

        if (!this.canSubmitForm(this.form))
            return;

        return this.Auth.signnup(this.data).catch((res) => {
            this.IgniteMessages.showError(null, res.data);
            this.setServerError(res.data);
        });
    }
}
