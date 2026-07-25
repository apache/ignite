

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
        if (_.isNil(attrs.javaIdentifier) || attrs.javaIdentifier !== 'true')
            return;

        /** @type {Array<string>} */
        const extraValidIdentifiers = scope.$eval(attrs.extraValidJavaIdentifiers) || [];

        ngModel.$validators.javaIdentifier = (value) => attrs.validationActive === 'false' ||
            _.isEmpty(value) || JavaTypes.validClassName(value) || extraValidIdentifiers.includes(value);

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
