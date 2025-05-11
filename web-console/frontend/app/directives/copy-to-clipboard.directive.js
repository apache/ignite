

/**
 * @param {ReturnType<typeof import('../services/CopyToClipboard.service').default>} CopyToClipboard
 */
export default function directive(CopyToClipboard) {
    return {
        restrict: 'A',
        /**
         * @param {ng.IScope} scope
         * @param {JQLite} element
         * @param {ng.IAttributes} attrs
         */
        link(scope, element, attrs) {
            element.bind('click', () => CopyToClipboard.copy(attrs.igniteCopyToClipboard));

            if (!document.queryCommandSupported('copy'))
                element.hide();
        }
    };
}

directive.$inject = ['IgniteCopyToClipboard'];
