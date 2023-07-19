

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
