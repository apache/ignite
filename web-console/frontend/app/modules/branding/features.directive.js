

const template = '<div class="features" ng-bind-html="features.html"></div>';

/**
 * @param {import('./branding.service').default} branding
 */
export default function factory(branding) {
    function controller() {
        const ctrl = this;

        ctrl.html = branding.featuresHtml;
    }

    return {
        restrict: 'E',
        template,
        controller,
        controllerAs: 'features',
        replace: true
    };
}

factory.$inject = ['IgniteBranding'];

