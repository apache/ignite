

/**
 * @param {import('./branding.service').default} branding
 */
export default function factory(branding) {
    function controller() {
        const ctrl = this;

        ctrl.termsState = branding.termsState;
    }

    return {
        restrict: 'A',
        controller,
        controllerAs: 'terms'
    };
}

factory.$inject = ['IgniteBranding'];
