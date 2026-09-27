

import template from './loading.pug';
import './loading.scss';

/**
 * @param {ReturnType<typeof import('./loading.service').default>} Loading
 * @param {ng.ICompileService} $compile
 */
export default function factory(Loading, $compile) {
    const link = (scope, element) => {
        const compiledTemplate = $compile(template);

        const build = () => {
            scope.position = scope.position || 'middle';

            const loading = compiledTemplate(scope);

            if (!scope.loading) {
                scope.loading = loading;

                Loading.add(scope.key || 'defaultSpinnerKey', scope.loading);
                element.append(scope.loading);
            }
        };

        build();
    };

    return {
        scope: {
            key: '@igniteLoading',
            text: '@?igniteLoadingText',
            class: '@?igniteLoadingClass',
            position: '@?igniteLoadingPosition'
        },
        restrict: 'A',
        link
    };
}

factory.$inject = ['IgniteLoading', '$compile'];
