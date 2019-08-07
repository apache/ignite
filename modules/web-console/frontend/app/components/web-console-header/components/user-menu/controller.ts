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

export default class UserMenu {
    static $inject = ['$rootScope', 'IgniteUserbar', 'AclService', '$state', 'gettingStarted'];

    constructor(
        private $root: ng.IRootScopeService,
        private IgniteUserbar: any,
        private AclService: any,
        private $state: any,
        private gettingStarted: any
    ) {}

    $onInit() {
        this.items = [
            {text: 'Profile', sref: 'base.settings.profile'},
            {text: 'Getting started', click: '$ctrl.gettingStarted.tryShow(true)'}
        ];

        const _rebuildSettings = () => {
            this.items.splice(2);

            if (this.AclService.can('admin_page'))
                this.items.push({text: 'Admin panel', sref: 'base.settings.admin'});

            this.items.push(...this.IgniteUserbar);

            if (this.AclService.can('logout'))
                this.items.push({text: 'Log out', sref: 'logout'});
        };

        if (this.$root.user)
            _rebuildSettings(null, this.$root.user);

        this.$root.$on('user', _rebuildSettings);
    }

    get user() {
        return this.$root.user;
    }
}
