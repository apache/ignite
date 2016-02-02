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
const templateTitle = `<label class= 'title'>{{::title.text}}</label>`;
import templatePoweredByApache from './powered-by-apache.jade!';

import angular from 'angular';

angular
    .module('ignite-console.logo', [

    ])
    .provider('igniteLogo', function() {
        let poweredBy = false;

        let url = '/images/ignite_logo.png';

        let title = 'Management console for Apache Ignite';

        this.url = function(_url) {
            url = _url;

            poweredBy = true;
        };

        this.title = function(_title) {
            title = _title;
        };

        this.$get = [function() {
            return {
                url,
                poweredBy,
                title
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
    }])
    .directive('igniteTitle', ['igniteLogo', function(igniteLogo) {
        function controller() {
            const ctrl = this;

            ctrl.text = igniteLogo.title;
        }

        return {
            restrict: 'E',
            template: templateTitle,
            controller,
            controllerAs: 'title',
            replace: true
        };
    }]);

