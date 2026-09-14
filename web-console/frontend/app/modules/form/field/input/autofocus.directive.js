

import _ from 'lodash';

/**
 * @param {ng.ITimeoutService} $timeout
 */
export default function factory($timeout) {
    /**
     * @param {ng.IScope} scope
     * @param {JQLite} el
     * @param {ng.IAttributes} attrs
     */
    const link = (scope, el, attrs) => {
        if (_.isUndefined(attrs.igniteFormFieldInputAutofocus) || attrs.igniteFormFieldInputAutofocus !== 'true')
            return;

        $timeout(() => el.focus(), 100);
    };

    return {
        restrict: 'A',
        link
    };
}

factory.$inject = ['$timeout'];
