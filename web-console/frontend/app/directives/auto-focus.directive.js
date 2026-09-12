

/**
 * Directive to auto-focus specified element.
 * @param {ng.ITimeoutService} $timeout
 */
export default function directive($timeout) {
    return {
        restrict: 'AC',
        /**
         * @param {ng.IScope} scope
         * @param {JQLite} element
         */
        link(scope, element) {
            $timeout(() => element[0].focus(), 100);
        }
    };
}

directive.$inject = ['$timeout'];
