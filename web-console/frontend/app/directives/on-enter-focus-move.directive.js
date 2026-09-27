

/**
 * Directive to move focus to specified element on ENTER key.
 * @param {ReturnType<typeof import('../services/Focus.service').default>} Focus
 */
export default function directive(Focus) {
    /**
     * @param {ng.IScope} scope
     * @param {JQLite} elem
     * @param {ng.IAttributes} attrs
     */
    function directive(scope, elem, attrs) {
        elem.on('keydown keypress', (event) => {
            if (event.which === 13) {
                event.preventDefault();

                Focus.move(attrs.igniteOnEnterFocusMove);
            }
        });
    }

    return directive;
}

directive.$inject = ['IgniteFocus'];
