

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
        if (_.isUndefined(attrs.javaBuiltInClass) || !attrs.javaBuiltInClass)
            return;

        ngModel.$validators.javaBuiltInClass = (value) => attrs.validationActive === 'false' ||
            JavaTypes.nonBuiltInClass(value);

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
