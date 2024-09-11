

import _ from 'lodash';

/**
 * @param {ng.IParseService} $parse
 */
export default function factory($parse) {
    /**
     * @param {ng.IScope} scope
     * @param {JQLite} el
     * @param {ng.IAttributes} attrs
     * @param {[ng.INgModelController]} [ngModel]
     */
    const link = (scope, el, attrs, [ngModel]) => {
        if (_.isUndefined(attrs.ignitePropertyUnique) || !attrs.ignitePropertyUnique)
            return;

        ngModel.$validators.ignitePropertyUnique = (value) => {
            const arr = $parse(attrs.ignitePropertyUnique)(scope);

            // Return true in case if array not exist, array empty.
            if (!value || !arr || !arr.length)
                return true;

            const key = value.split('=')[0];
            const idx = _.findIndex(arr, (item) => item.split('=')[0] === key);

            // In case of new element check all items.
            if (attrs.name === 'new')
                return idx < 0;

            // Check for $index in case of editing in-place.
            return (_.isNumber(scope.$index) && (idx < 0 || scope.$index === idx));
        };
    };

    return {
        restrict: 'A',
        link,
        require: ['ngModel']
    };
}

factory.$inject = ['$parse'];
