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

import {default as ListEditable} from '../../controller';
import noItemsTemplate from './no-items-template.pug';
import hasItemsTemplate from './has-items-template.pug';
import './style.scss';

/**
 * Adds "add new item" button to list-editable-no-items slot and after list-editable
 */
export class ListEditableAddItemButton<T> {
    /** 
     * Template for button that's inserted after list-editable
     */
    static hasItemsTemplate: string = hasItemsTemplate;
    _listEditable: ListEditable<T>;
    labelSingle: string;
    labelMultiple: string;
    _addItem: ng.ICompiledExpression;

    static $inject = ['$compile', '$scope'];

    constructor(private $compile: ng.ICompileService, private $scope: ng.IScope) {}

    $onDestroy() {
        this._listEditable = this._hasItemsButton = null;
    }

    $postLink() {
        this.$compile(ListEditableAddItemButton.hasItemsTemplate)(this.$scope, (hasItemsButton) => {
            hasItemsButton.insertAfter(this._listEditable.$element);
        });
    }

    get hasItems() {
        return !this._listEditable.ngModel.$isEmpty(this._listEditable.ngModel.$viewValue);
    }

    addItem() {
        return this._addItem({
            $edit: this._listEditable.ngModel.editListItem.bind(this._listEditable),
            $editLast: (length) => this._listEditable.ngModel.editListIndex(length - 1)
        });
    }
}

export default {
    controller: ListEditableAddItemButton,
    require: {
        _listEditable: '^listEditable'
    },
    bindings: {
        _addItem: '&addItem',
        labelSingle: '@',
        labelMultiple: '@'
    },
    template: noItemsTemplate
};
