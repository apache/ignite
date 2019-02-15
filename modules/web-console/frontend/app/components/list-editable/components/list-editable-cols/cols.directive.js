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
