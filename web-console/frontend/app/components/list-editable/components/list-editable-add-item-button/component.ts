

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
