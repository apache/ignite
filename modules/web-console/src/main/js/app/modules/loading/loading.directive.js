/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import templateUrl from './loading.jade';
import './loading.css';

export default ['igniteLoading', ['IgniteLoading', '$templateCache', '$compile', (Loading, $templateCache, $compile) => {
    const link = (scope, element) => {
        const compiledTemplate = $compile($templateCache.get(templateUrl));

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
}]];
