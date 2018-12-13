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

/**
 * Directive to bind ESC key press with some user action.
 * @param {ng.ITimeoutService} $timeout
 */
export default function directive($timeout) {
    /**
     * @param {ng.IScope} scope
     * @param {JQLite} elem
     * @param {ng.IAttributes} attrs
     */
    function directive(scope, elem, attrs) {
        elem.on('keydown keypress', (event) => {
            if (event.which === 27) {
                scope.$apply(() => $timeout(() => scope.$eval(attrs.igniteOnEscape)));

                event.preventDefault();
            }
        });

        // Removes bound events in the element itself when the scope is destroyed.
        scope.$on('$destroy', () => elem.off('keydown keypress'));
    }

    return directive;
}

directive.$inject = ['$timeout'];
