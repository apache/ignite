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

import template from './cols.template.pug';
import './cols.style.scss';

/**
 * A column definition.
 *
 * @typedef {Object} IListEditableColDef
 * @prop {string} [name] - optional name to display at column head
 * @prop {string} [cellClass] - CSS class to assign to column cells
 * @prop {string} [tip] - optional tip to display at column head
 */
export class ListEditableColsController {
    /** @type {Array<IListEditableColDef>} - column definitions */
    colDefs;
    /** @type {string} - optional class to assign to rows */
    rowClass;
    /** @type {ng.INgModelController} */
    ngModel;

    static $inject = ['$compile', '$element', '$scope'];

    /**
     * @param {ng.ICompileService} $compile
     * @param {JQLite} $element
     * @param {ng.IScope} $scope
     */
    constructor($compile, $element, $scope) {
        this.$compile = $compile;
        this.$element = $element;
        this.$scope = $scope;
    }

    $postLink() {
        this.$compile(template)(this.$scope.$new(true), (clone, scope) => {
            scope.$ctrl = this;

            this.$element[0].parentElement.insertBefore(clone[0], this.$element[0]);
        });
    }

    $onDestroy() {
        this.$element = null;
    }
}

/** @returns {ng.IDirective} */
export default function listEditableCols() {
    return {
        controller: ListEditableColsController,
        controllerAs: '$colsCtrl',
        require: {
            ngModel: 'ngModel'
        },
        bindToController: {
            colDefs: '<listEditableCols',
            rowClass: '@?listEditableColsRowClass',
            ngDisabled: '<?'
        }
    };
}
