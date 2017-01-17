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

import angular from 'angular';

/**
 * Special decorator that fix problem in AngularStrap $tooltip in special case.
 * Case: when tooltip is shown on table row remove button and user click this button.
 * If this problem will be fixed in AngularStrap we can remove this delegate.
 */
angular.module('mgcrea.ngStrap.tooltip')
    .decorator('$tooltip', ['$delegate', ($delegate) => {
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

                if (angular.isDefined(options.onBeforeHide) && angular.isFunction(options.onBeforeHide))
                    options.onBeforeHide($tooltip);

                $tooltip.$isShown = scope.$isShown = false;
                scope.$$phase || (scope.$root && scope.$root.$$phase) || scope.$digest();
            };

            return $tooltip;
        }

        return TooltipFactoryDecorated;
    }]);
