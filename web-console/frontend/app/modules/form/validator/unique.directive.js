

import {ListEditableTransclude} from 'app/components/list-editable/components/list-editable-transclude/directive';
import isNumber from 'lodash/fp/isNumber';
import get from 'lodash/fp/get';

class Controller {
    /** @type {ng.INgModelController} */
    ngModel;
    /** @type {ListEditableTransclude} */
    listEditableTransclude;
    /** @type {Array} */
    items;
    /** @type {string?} */
    key;
    /** @type {Array<string>} */
    skip;

    static $inject = ['$scope'];

    /**
     * @param {ng.IScope} $scope
     */
    constructor($scope) {
        this.$scope = $scope;
    }

    $onInit() {
        const isNew = this.key && this.key.startsWith('new');
        const shouldNotSkip = (item) => get(this.skip[0], item) !== get(...this.skip);

        this.ngModel.$validators.igniteUnique = (value) => {
            const matches = (item) => (this.key ? item[this.key] : item) === value;

            if (!this.skip) {
                // Return true in case if array not exist, array empty.
                if (!this.items || !this.items.length)
                    return true;

                const idx = this.items.findIndex(matches);

                // In case of new element check all items.
                if (isNew)
                    return idx < 0;

                // Case for new component list editable.
                const $index = this.listEditableTransclude
                    ? this.listEditableTransclude.$index
                    : isNumber(this.$scope.$index) ? this.$scope.$index : void 0;

                // Check for $index in case of editing in-place.
                return (isNumber($index) && (idx < 0 || $index === idx));
            }
            // TODO: converge both branches, use $index as idKey
            return !(this.items || []).filter(shouldNotSkip).some(matches);
        };
    }

    $onChanges(changes) {
        this.ngModel.$validate();
    }
}

export default () => {
    return {
        controller: Controller,
        require: {
            ngModel: 'ngModel',
            listEditableTransclude: '?^listEditableTransclude'
        },
        bindToController: {
            items: '<igniteUnique',
            key: '@?igniteUniqueProperty',
            skip: '<?igniteUniqueSkip'
        }
    };
};
