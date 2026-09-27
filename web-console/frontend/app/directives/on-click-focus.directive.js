

/**
 * Directive to describe element that should be focused on click.
 * @param {ReturnType<typeof import('../services/Focus.service').default>} Focus
 */
export default function directive(Focus) {
    /**
     * @param {ng.IScope} scope
     * @param {JQLite} elem
     * @param {ng.IAttributes} attrs
     */
    function directive(scope, elem, attrs) {
        elem.on('click', () => Focus.move(attrs.igniteOnClickFocus));

        // Removes bound events in the element itself when the scope is destroyed
        scope.$on('$destroy', () => elem.off('click'));
    }

    return directive;
}

directive.$inject = ['IgniteFocus'];
