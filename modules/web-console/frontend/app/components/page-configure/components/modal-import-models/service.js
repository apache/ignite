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

export default class ModalImportModels {
    static $inject = ['$modal', 'AgentManager', '$uiRouter'];
    constructor($modal, AgentManager, $uiRouter) {
        Object.assign(this, {$modal, AgentManager, $uiRouter});
    }
    _goToDynamicState() {
        if (this._state) this.$uiRouter.stateRegistry.deregister(this._state);
        this._state = this.$uiRouter.stateRegistry.register({
            name: 'importModels',
            parent: this.$uiRouter.stateService.current,
            onEnter: () => {
                this._open();
            },
            onExit: () => {
                this._modal && this._modal.hide();
            }
        });
        return this.$uiRouter.stateService.go(this._state, this.$uiRouter.stateService.params);
    }
    _open() {
        this._modal = this.$modal({
            template: `
                <modal-import-models
                    on-hide='$ctrl.$state.go("^")'
                    cluster-id='$ctrl.$state.params.clusterID'
                ></modal-import-models>
            `,
            controller: ['$state', function($state) {this.$state = $state;}],
            controllerAs: '$ctrl',
            show: false
        });
        return this.AgentManager.startAgentWatch('Back', this.$uiRouter.globals.current.name)
        .then(() => this._modal.$promise)
        .then(() => this._modal.show());
    }
    open() {
        this._goToDynamicState();
    }
}
