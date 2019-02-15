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
            this.setServerError(res.data);
        });
    }
}
