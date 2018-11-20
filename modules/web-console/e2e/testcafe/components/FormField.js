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
            this._selector = Selector(() => {
                return Array
                    .from(window.document.querySelectorAll(this.constructor.LABEL_SELECTOR))
                    .filter((el) => el.textContent.contains(label))
                    .map((el) => el.parent(this.constructor.ROOT_SELECTOR))
                    .pop();
            });
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
}

/**
 * Not really a custom field, use for form fields at login and profile screens, these don't have "ignite" prefix
 */
export class CustomFormField extends FormField {
    static ROOT_SELECTOR = '.form-field';
    static LABEL_SELECTOR = '.form-field__label';
    static ERRORS_SELECTOR = '.form-field__errors';
}
