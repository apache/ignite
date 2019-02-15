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

import {Selector, t} from 'testcafe';
import {AngularJSSelector} from 'testcafe-angular-selectors';

export class FormField {
    static ROOT_SELECTOR = '.form-field';
    static LABEL_SELECTOR = '.form-field__label';
    static CONTROL_SELECTOR = '[ng-model]';
    static ERRORS_SELECTOR = '.form-field__errors';

    /** @type {ReturnType<Selector>} */
    _selector;

    constructor({id = '', label = '', model = ''} = {}) {
        if (!id && !label && !model) throw new Error('ID, label or model are required');
        if (id)
            this._selector = Selector(`#${id}`).parent(this.constructor.ROOT_SELECTOR);
        else if (label) {
            this._selector = Selector((LABEL_SELECTOR, ROOT_SELECTOR, label) => {
                return [].slice.call((window.document.querySelectorAll(LABEL_SELECTOR)))
                    .filter((el) => el.textContent.includes(label))
                    .map((el) => el.closest(ROOT_SELECTOR))
                    .pop();
            })(this.constructor.LABEL_SELECTOR, this.constructor.ROOT_SELECTOR, label);
        } else if (model)
            this._selector = AngularJSSelector.byModel(model).parent(this.constructor.ROOT_SELECTOR);

        this.label = this._selector.find(this.constructor.LABEL_SELECTOR);
        this.control = this._selector.find(this.constructor.CONTROL_SELECTOR);
        this.errors = this._selector.find(this.constructor.ERRORS_SELECTOR);
    }
    /**
     * Selects dropdown option
     * @param {string} label
     */
    async selectOption(label) {
        await t
            .click(this.control)
            .click(Selector('.bssm-item-button').withText(label));
    }
    /**
     * Get error element by error type
     * @param {string} errorType
     */
    getError(errorType) {
        // return this._selector.find(`.form-field__error`)
        return this._selector.find(`[ng-message="${errorType}"]`);
    }
    get selectedOption() {
        return this.control.textContent;
    }
    get postfix() {
        return this._selector.find('[data-postfix]').getAttribute('data-postfix');
    }
}

/**
 * Not really a custom field, use for form fields at login and profile screens, these don't have "ignite" prefix
 */
export class CustomFormField extends FormField {
    static ROOT_SELECTOR = '.form-field';
    static LABEL_SELECTOR = '.form-field__label';
    static ERRORS_SELECTOR = '.form-field__errors';
    constructor(...args) {
        super(...args);
        this.errors = this.errors.addCustomMethods({
            hasError(errors, errorMessage) {
                return !!errors.querySelectorAll(`.form-field__error [data-title*="${errorMessage}"]`).length;
            }
        });
    }
}
