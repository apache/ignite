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

import {Selector, t} from 'testcafe'
import {FormField} from './FormField'

const addItemButton = Selector(value => {
    value = value();
    const innerButton = value.querySelector('.le-row:not(.ng-hide) list-editable-add-item-button [ng-click]');

    if (innerButton)
        return innerButton;

    /** @type {Element} */
    const outerButton = value.nextElementSibling;

    if (outerButton.getAttribute('ng-click') === '$ctrl.addItem()')
        return outerButton;
});

export class ListEditableItem {
    /**
     * @param {Selector} selector
     * @param {Object.<string, {id: string}>} fieldsMap
     */
    constructor(selector, fieldsMap = {}) {
        this._selector = selector;
        this._fieldsMap = fieldsMap;
        /** @type {SelectorAPI} */
        this.editView = this._selector.find('list-editable-item-edit');
        /** @type {SelectorAPI} */
        this.itemView = this._selector.find('list-editable-item-view');
        /** @type {Object.<string, FormField>} Inline form fields */
        this.fields = Object.keys(fieldsMap).reduce((acc, key) => ({...acc, [key]: new FormField(this._fieldsMap[key])}), {})
    }
    async startEdit() {
        await t.click(this.itemView)
    }
    async stopEdit() {
        await t.click('.wrapper')
    }
    /**
     * @param {number} index
     */
    getItemViewColumn(index) {
        return this.itemView.child(index)
    }
}

export class ListEditable {
    static ADD_ITEM_BUTTON_SELECTOR = '[ng-click="$ctrl.addItem()"]';
    /** @param {SelectorAPI} selector */
    constructor(selector, fieldsMap) {
        this._selector = selector;
        this._fieldsMap = fieldsMap;
        this.addItemButton = Selector(addItemButton(selector))
    }

    async addItem() {
        await t.click(this.addItemButton)
    }

    /**
     * @param {number} index Zero-based index of item in the list
     */
    getItem(index) {
        return new ListEditableItem(this._selector.find(`.le-body>.le-row[ng-repeat]`).nth(index), this._fieldsMap)
    }
}
