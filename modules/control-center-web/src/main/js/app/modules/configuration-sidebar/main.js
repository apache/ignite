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

angular
.module('ignite-web-console.configuration.sidebar', [

])
.provider('igniteConfigurationSidebar', function() {
    var items = [
        { label: 'Clusters', href: '/configuration/clusters' },
        { label: 'Caches', href: '/configuration/caches' },
        { label: 'Metadata', href: '/configuration/metadata' },
        { label: 'IGFS', href: '/configuration/igfs' }
    ];

    this.push = function(data) {
        items.push(data);
    };

    this.$get = [function() {
        var r = angular.copy(items);

        r.push({ label: 'Summary', href: '/configuration/summary' });

        return r;
    }]
})
.directive('igniteConfigurationSidebar', function(igniteConfigurationSidebar) {
    function controller() {
        var ctrl = this;

        ctrl.items = igniteConfigurationSidebar;
    }

    return {
        restrict: 'A',
        controller: controller,
        controllerAs: 'sidebar'
    }
});
