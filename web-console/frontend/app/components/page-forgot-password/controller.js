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

export default class PageForgotPassword {
    /** @type {string} Optional email to populate form with */
    email;
    /** @type {import('./types').IForgotPasswordFormController} */
    form;
    /** @type {import('./types').IForgotPasswordData} */
    data = {email: null};
    /** @type {string} */
    serverError = null;
    /** @type {JQLite} */
    el;

    static $inject = ['Auth', 'IgniteMessages', 'IgniteFormUtils', '$element'];

    /**
     * @param {import('app/modules/user/Auth.service').default} Auth
     */
    constructor(Auth, IgniteMessages, IgniteFormUtils, el) {
        this.Auth = Auth;
        this.IgniteMessages = IgniteMessages;
        this.IgniteFormUtils = IgniteFormUtils;
        this.el = el;
    }
    /** @param {import('./types').IForgotPasswordFormController} form */
    canSubmitForm(form) {
        return form.$error.server ? true : !form.$invalid;
    }
    $postLink() {
        this.el.addClass('public-page');
        this.form.email.$validators.server = () => !this.serverError;
    }

    /** @param {string} error */
    setServerError(error) {
        this.serverError = error;
        this.form.email.$validate();
    }

    /**
     * @param {{email: ng.IChangesObject<string>}} changes
     */
    $onChanges(changes) {
        if ('email' in changes) this.data.email = changes.email.currentValue;
    }
    remindPassword() {
        this.IgniteFormUtils.triggerValidation(this.form);

        this.setServerError(null);

        if (!this.canSubmitForm(this.form))
            return;

        return this.Auth.remindPassword(this.data.email).catch((res) => {
            this.IgniteMessages.showError(null, res.data);
            this.setServerError(res.data.message);
        });
    }
}
