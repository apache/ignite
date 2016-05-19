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
            const delegate = $delegate(element, config);

            const scope = delegate.$scope;

            const options = delegate.$options;

            const hideWraped = delegate.hide;

            delegate.hide = (blur) => {
                if (!delegate.$isShown)
                    return;

                if (delegate.$element !== null)
                    return hideWraped(blur);

                scope.$emit(options.prefixEvent + '.hide.before', delegate);

                if (angular.isDefined(options.onBeforeHide) && angular.isFunction(options.onBeforeHide))
                    options.onBeforeHide(delegate);

                delegate.$isShown = scope.$isShown = false;
                scope.$$phase || (scope.$root && scope.$root.$$phase) || scope.$digest();
            };

            return delegate;
        }

        return TooltipFactoryDecorated;
    }]);
