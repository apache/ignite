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

import angular from 'angular';
import _ from 'lodash';

/**
 * Decorator that fix problem in AngularStrap $tooltip.
 */
export default angular
    .module('mgcrea.ngStrap.tooltip')
    /**
     * Don't hide tooltip when mouse move from element to tooltip.
     */
    .decorator('$tooltip', ['$delegate', function($delegate) {
        function TooltipFactoryDecorated(element, config) {
            let tipElementEntered = false;

            config.onShow = ($tooltip) => {
                // Workaround for tooltip detection.
                if ($tooltip.$element && $tooltip.$options.trigger === 'click hover') {
                    $tooltip.$element.on('mouseenter', () => tipElementEntered = true);
                    $tooltip.$element.on('mouseleave', () => {
                        tipElementEntered = false;

                        $tooltip.leave();
                    });
                }
            };

            const $tooltip = $delegate(element, config);

            const scope = $tooltip.$scope;
            const options = $tooltip.$options;

            const _hide = $tooltip.hide;

            $tooltip.hide = (blur) => {
                if (!$tooltip.$isShown || tipElementEntered)
                    return;

                if ($tooltip.$element) {
                    $tooltip.$element.off('mouseenter');
                    $tooltip.$element.off('mouseleave');

                    return _hide(blur);
                }

                scope.$emit(options.prefixEvent + '.hide.before', $tooltip);

                if (!_.isUndefined(options.onBeforeHide) && _.isFunction(options.onBeforeHide))
                    options.onBeforeHide($tooltip);

                $tooltip.$isShown = scope.$isShown = false;
                scope.$$phase || (scope.$root && scope.$root.$$phase) || scope.$digest();
            };

            return $tooltip;
        }

        return TooltipFactoryDecorated;
    }])
    /**
     * Set width for dropdown as for element.
     */
    .decorator('$tooltip', ['$delegate', ($delegate) => {
        return function(el, config) {
            const $tooltip = $delegate(el, config);

            $tooltip.$referenceElement = el;
            $tooltip.destroy = _.flow($tooltip.destroy, () => $tooltip.$referenceElement = null);
            $tooltip.$applyPlacement = _.flow($tooltip.$applyPlacement, () => {
                if (!$tooltip.$element)
                    return;

                const refWidth = $tooltip.$referenceElement[0].getBoundingClientRect().width;
                const elWidth = $tooltip.$element[0].getBoundingClientRect().width;

                if (refWidth > elWidth) {
                    $tooltip.$element.css({
                        width: refWidth,
                        maxWidth: 'initial'
                    });
                }
            });

            return $tooltip;
        };
    }]);
