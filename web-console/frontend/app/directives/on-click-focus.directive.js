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

/**
 * Directive to describe element that should be focused on click.
 * @param {ReturnType<typeof import('../services/Focus.service').default>} Focus
 */
export default function directive(Focus) {
    /**
     * @param {ng.IScope} scope
     * @param {JQLite} elem
     * @param {ng.IAttributes} attrs
     */
    function directive(scope, elem, attrs) {
        elem.on('click', () => Focus.move(attrs.igniteOnClickFocus));

        // Removes bound events in the element itself when the scope is destroyed
        scope.$on('$destroy', () => elem.off('click'));
    }

    return directive;
}

directive.$inject = ['IgniteFocus'];
