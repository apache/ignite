/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

export default function() {
    return {
        restrict: 'A',
        controller: class {
            static $inject = ['$scope', '$attrs', '$sce', '$element', '$window', 'IgniteIcon'];

            /**
             * @param {ng.IScope} $scope     
             * @param {ng.IAttributes} $attrs     
             * @param {ng.ISCEService} $sce       
             * @param {JQLite} $element   
             * @param {ng.IWindowService} $window    
             * @param {import('./service').default} IgniteIcon 
             */
            constructor($scope, $attrs, $sce, $element, $window, IgniteIcon) {
                this.$scope = $scope;
                this.$attrs = $attrs;
                this.$sce = $sce;
                this.$element = $element;
                this.$window = $window;
                this.IgniteIcon = IgniteIcon;
            }

            $onInit() {
                this.off = this.$scope.$on('$locationChangeSuccess', (e, url) => {
                    this.render(this.getFragmentURL(url));
                });

                this.wrapper = document.createElement('div');
            }

            $onDestroy() {
                this.$element = this.$window = this.wrapper = null;

                this.off();
            }

            $postLink() {
                /** @type {string} */
                this.name = this.$attrs.igniteIcon;
                this.$element.attr('viewBox', this.IgniteIcon.getIcon(this.name).viewBox);

                this.render(this.getFragmentURL());
            }

            getFragmentURL(url = this.$window.location.href) {
                // All browsers except for Chrome require absolute URL of a fragment.
                // Combine that with base tag and HTML5 navigation mode and you get this.
                return `${url.split('#')[0]}#${this.name}`;
            }

            /**
             * @param {string} url 
             */
            render(url) {
                // templateNamespace: 'svg' does not work in IE11
                this.wrapper.innerHTML = `<svg><use xlink:href="${url}" href="${url}" /></svg>`;

                Array.from(this.wrapper.childNodes[0].childNodes).forEach((n) => {
                    this.$element.empty().append(n);
                });
            }
        }
    };
}
