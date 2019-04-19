/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
