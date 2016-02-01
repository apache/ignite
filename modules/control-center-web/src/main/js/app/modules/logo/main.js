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

import templateLogo from './logo.jade!';
import templatePoweredByApache from './powered-by-apache.jade!';

import angular from 'angular';

angular
    .module('ignite-console.logo', [

    ])
    .provider('igniteLogo', function() {
        let _poweredBy = false;

        let _url = '/images/ignite_logo.png';

        this.url = function(url) {
            _url = url;

            _poweredBy = true;
        };

        this.$get = [function() {
            return {
                url: _url,
                poweredBy: _poweredBy
            };
        }];
    })
    .directive('ignitePoweredByApache', ['igniteLogo', function(igniteLogo) {
        function controller() {
            const ctrl = this;

            ctrl.show = igniteLogo.poweredBy;
        }

        return {
            restrict: 'E',
            template: templatePoweredByApache,
            controller,
            controllerAs: 'poweredBy',
            replace: true
        };
    }])
    .directive('igniteLogo', ['igniteLogo', function(igniteLogo) {
        function controller() {
            const ctrl = this;

            ctrl.url = igniteLogo.url;
        }

        return {
            restrict: 'E',
            template: templateLogo,
            controller,
            controllerAs: 'logo',
            replace: true
        };
    }]);
