

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
        if (_.isNil(attrs.javaKeywords) || attrs.javaKeywords === 'false')
            return;

        const packageOnly = attrs.javaPackageName === 'package-only';

        ngModel.$validators.javaKeywords = (value) => attrs.validationActive === 'false' ||
            _.isEmpty(value) || !JavaTypes.validClassName(value) ||
            (!packageOnly && !JavaTypes.packageSpecified(value)) ||
            _.findIndex(value.split('.'), JavaTypes.isKeyword) < 0;

        if (attrs.validationActive !== 'always')
            attrs.$observe('validationActive', () => ngModel.$validate());
    };

    return {
        restrict: 'A',
        link,
        require: ['ngModel']
    };
}

factory.$inject = ['JavaTypes'];
