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

export default ['igniteUserbar', [function() {
    return {
        restrict: 'A',
        controller: ['$rootScope', 'IgniteUserbar', 'AclService', function($root, IgniteUserbar, AclService) {
            const ctrl = this;

            ctrl.items = [
                {text: 'Profile', sref: 'settings.profile'},
                {text: 'Getting started', click: 'gettingStarted.tryShow(true)'}
            ];

            const _rebuildSettings = () => {
                ctrl.items.splice(2);

                if (AclService.can('admin_page'))
                    ctrl.items.push({text: 'Admin panel', sref: 'settings.admin'});

                ctrl.items.push(...IgniteUserbar);

                if (AclService.can('logout'))
                    ctrl.items.push({text: 'Log out', sref: 'logout'});
            };

            if ($root.user)
                _rebuildSettings(null, $root.user);

            $root.$on('user', _rebuildSettings);
        }],
        controllerAs: 'userbar'
    };
}]];
