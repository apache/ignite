

import _ from 'lodash';

/**
 * @param {import('app/services/JavaTypes.service').default} JavaTypes
 */
export default function factory(JavaTypes) {
    /**
     * @param {ng.IScope} scope
     * @param {JQLite} el
     * @param {ng.IAttributes} attrs
     * @param {[ng.INgModelController]} [ngModel]
     */
    const link = (scope, el, attrs, [ngModel]) => {
        if (_.isNil(attrs.javaPackageName) || attrs.javaPackageName === 'false')
            return;

        ngModel.$validators.javaPackageName = (value) => _.isEmpty(value) || JavaTypes.validPackage(value);
    };

    return {
        restrict: 'A',
        link,
        require: ['ngModel']
    };
}

factory.$inject = ['JavaTypes'];
