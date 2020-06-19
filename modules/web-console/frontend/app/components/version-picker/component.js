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

import _ from 'lodash';
import template from './template.pug';
import './style.scss';

export default {
    template,
    controller: class {
        static $inject = ['IgniteVersion', '$scope'];

        /**
         * @param {import('app/services/Version.service').default} Version
         * @param {ng.IRootScopeService} $scope
         */
        constructor(Version, $scope) {
            this.currentSbj = Version.currentSbj;
            this.supportedVersions = Version.supportedVersions;

            const dropdownToggle = (active) => {
                this.isActive = active;

                // bs-dropdown does not call apply on callbacks
                $scope.$apply();
            };

            this.onDropdownShow = () => dropdownToggle(true);
            this.onDropdownHide = () => dropdownToggle(false);
        }

        $onInit() {
            this.dropdown = _.map(this.supportedVersions, (ver) => ({
                text: ver.label,
                click: () => this.currentSbj.next(ver)
            }));

            this.currentSbj.subscribe({
                next: (ver) => this.currentVersion = ver.label
            });
        }
    }
};
