

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
        if (_.isNil(attrs.javaPackageSpecified) || attrs.javaPackageSpecified === 'false')
            return;

        const allowBuiltIn = attrs.javaPackageSpecified === 'allow-built-in';

        ngModel.$validators.javaPackageSpecified = (value) => attrs.validationActive === 'false' ||
            _.isEmpty(value) ||
            !JavaTypes.validClassName(value) || JavaTypes.packageSpecified(value) ||
            (allowBuiltIn && !JavaTypes.nonBuiltInClass(value));

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
