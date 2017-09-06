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

import * as icons from '../../../public/images/icons/index.js';

export default function() {
    return {
        restrict: 'A',
        controller: class {
            static $inject = ['$scope', '$attrs', '$sce', '$element', '$window'];

            constructor($scope, $attrs, $sce, $element, $window) {
                Object.assign(this, {$scope, $attrs, $sce, $element, $window});
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
                this.name = this.$attrs.igniteIcon;
                this.$element.attr('viewBox', icons[this.name].viewBox);

                this.render(this.getFragmentURL());
            }

            getFragmentURL(url = this.$window.location.href) {
                // All browsers except for Chrome require absolute URL of a fragment.
                // Combine that with base tag and HTML5 navigation mode and you get this.
                return `${url.split('#')[0]}#${this.name}`;
            }

            render(url) {
                // templateNamespace: 'svg' does not work in IE11
                this.wrapper.innerHTML = `<svg><use xlink:href="${url}" href="${url}" /></svg>`;

                Array.from(this.wrapper.childNodes[0].childNodes).forEach((n) => {
                    this.$element[0].appendChild(n);
                });
            }
        }
    };
}
