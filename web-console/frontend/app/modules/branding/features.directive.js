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

