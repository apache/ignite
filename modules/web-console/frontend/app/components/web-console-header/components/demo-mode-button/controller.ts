/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {StateService} from '@uirouter/angularjs';
import {default as LegacyConfirmFactory} from 'app/services/Confirm.service';

export default class DemoModeButton {
    static $inject = ['$rootScope', '$state', '$window', 'IgniteConfirm', 'AgentManager', 'IgniteMessages'];

    constructor(
        private $root: ng.IRootScopeService,
        private $state: StateService,
        private $window: ng.IWindowService,
        private Confirm: ReturnType<typeof LegacyConfirmFactory>,
        private agentMgr: AgentManager,
        private Messages
    ) {}

    private _openTab(stateName: string) {
        this.$window.open(this.$state.href(stateName, {}), '_blank');
    }

    startDemo() {
        const connectionState = this.agentMgr.connectionSbj.getValue();
        const disconnected = _.get(connectionState, 'state') === 'AGENT_DISCONNECTED';
        const demoEnabled = _.get(connectionState, 'hasDemo');

        if (disconnected || demoEnabled || _.isNil(demoEnabled)) {
            if (!this.$root.user.demoCreated)
                return this._openTab('demo.reset');

            this.Confirm.confirm('Would you like to continue with previous demo session?', true, false)
                .then((resume) => {
                    if (resume)
                        return this._openTab('demo.resume');

                    this._openTab('demo.reset');
                });
        }
        else
            this.Messages.showError('Demo mode disabled by administrator');
    }
}
