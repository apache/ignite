

import _ from 'lodash';

export default () => {
    /**
     * @param {ng.IScope} scope
     * @param {JQLite} el
     * @param {ng.IAttributes} attrs
     * @param {[ng.INgModelController]} [ngModel]
     */
    const link = (scope, el, attrs, [ngModel]) => {
        if (_.isUndefined(attrs.ignitePropertyValueSpecified) || !attrs.ignitePropertyValueSpecified)
            return;

        ngModel.$validators.ignitePropertyValueSpecified = (value) => value ? value.indexOf('=') > 0 : true;
    };

    return {
        restrict: 'A',
        link,
        require: ['ngModel']
    };
};
