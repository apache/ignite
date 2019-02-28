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
import directive from './directive';
import flow from 'lodash/flow';

export default angular
    .module('ignite-console.page-configure.pc-ui-grid-filters', ['ui.grid'])
    .decorator('$tooltip', ['$delegate', ($delegate) => {
        return function(el, config) {
            const instance = $delegate(el, config);
            instance.$referenceElement = el;
            instance.destroy = flow(instance.destroy, () => instance.$referenceElement = null);
            instance.$applyPlacement = flow(instance.$applyPlacement, () => {
                if (!instance.$element)
                    return;

                const refWidth = instance.$referenceElement[0].getBoundingClientRect().width;
                const elWidth = instance.$element[0].getBoundingClientRect().width;

                if (refWidth > elWidth) {
                    instance.$element.css({
                        width: refWidth,
                        maxWidth: 'initial'
                    });
                }
            });
            return instance;
        };
    }])
    .directive('pcUiGridFilters', directive);
