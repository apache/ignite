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

angular
.module('ignite-console.userbar', [

])
.provider('igniteSettings', function() {
    const items = [];

    this.push = function(data) {
        items.push(data);
    };

    this.$get = [function() {
        return items;
    }];
})
.directive('igniteSettings', function() {
    return {
        restrict: 'A',
        controller: ['$rootScope', 'igniteSettings', function($root, igniteSettings) {
            const ctrl = this;

            ctrl.items = [{text: 'Profile', sref: 'settings.profile'}];

            const _rebuildSettings = (event, user) => {
                ctrl.items.splice(1);

                if (!user.becomeUsed && user.admin)
                    ctrl.items.push({text: 'Admin Panel', sref: 'settings.admin'});

                ctrl.items.push(...igniteSettings);

                if (!user.becomeUsed)
                    ctrl.items.push({text: 'Log Out', sref: 'logout'});
            };

            if ($root.user)
                _rebuildSettings(null, $root.user);

            $root.$on('user', _rebuildSettings);
        }],
        controllerAs: 'settings'
    };
});
