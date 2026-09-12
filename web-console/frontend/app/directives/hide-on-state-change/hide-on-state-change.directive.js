

/**
 * @param {import('@uirouter/angularjs').TransitionService} $transitions
 */
export default function directive($transitions) {
    /**
     * @param {ng.IScope} scope
     * @param {JQLite} element
     */
    const link = (scope, element) => {
        $transitions.onSuccess({}, () => {element.fadeOut('slow');});
    };

    return {
        restrict: 'AE',
        link
    };
}

directive.$inject = ['$transitions'];
