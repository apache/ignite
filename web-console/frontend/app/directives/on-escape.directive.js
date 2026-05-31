

/**
 * Directive to bind ESC key press with some user action.
 * @param {ng.ITimeoutService} $timeout
 */
export default function directive($timeout) {
    /**
     * @param {ng.IScope} scope
     * @param {JQLite} elem
     * @param {ng.IAttributes} attrs
     */
    function directive(scope, elem, attrs) {
        elem.on('keydown keypress', (event) => {
            if (event.which === 27) {
                scope.$apply(() => $timeout(() => scope.$eval(attrs.igniteOnEscape)));

                event.preventDefault();
            }
        });

        // Removes bound events in the element itself when the scope is destroyed.
        scope.$on('$destroy', () => elem.off('keydown keypress'));
    }

    return directive;
}

directive.$inject = ['$timeout'];
