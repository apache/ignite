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
.module('ignite-console.userbar', [

])
.provider('igniteUserbar', function() {
    var items = [];

    this.push = function(data) {
        items.push(data);
    };

    this.$get = [function() {
        return items;
    }]
})
.directive('igniteUserbar', function() {
    return {
        restrict: 'A',
        controller: ['$rootScope', 'igniteUserbar', function ($root, igniteUserbar) {
            var ctrl = this;

            ctrl.items = [{text: 'Profile', href: '/profile'}];
            ctrl.customItems = igniteUserbar;

            var _rebuildUserbar = function (event, user) {
                ctrl.items.splice(1);

                if (!user.becomeUsed && user.admin)
                    ctrl.items.push({text: 'Admin Panel', href: '/admin'});

                ctrl.items.push.apply(ctrl.items, ctrl.customItems);

                if (!user.becomeUsed)
                    ctrl.items.push({text: 'Log Out', href: '/logout'});
            };

            if ($root.user)
                _rebuildUserbar(undefined, $root.user);

            $root.$on('user', _rebuildUserbar);
        }],
        controllerAs: 'userbar'
    }
});
