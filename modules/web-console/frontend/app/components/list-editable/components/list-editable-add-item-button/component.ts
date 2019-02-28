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
