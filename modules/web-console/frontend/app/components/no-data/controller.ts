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

import _ from 'lodash';
import {of} from 'rxjs';
import {distinctUntilChanged, switchMap} from 'rxjs/operators';

import AgentManager from 'app/modules/agent/AgentManager.service';
import AgentModal from 'app/modules/agent/AgentModal.service';

export default class NoDataCmpCtrl {
    static $inject = ['$translate', 'AgentManager', 'AgentModal'];

    connectionState$ = this.AgentManager.connectionSbj.pipe(
        switchMap((sbj) => {
            if (!this.AgentManager.isDemoMode() && !_.isNil(sbj.cluster) && sbj.cluster.active === false)
                return of('CLUSTER_INACTIVE');

            return of(sbj.state);
        }),
        distinctUntilChanged()
    );

    backState = '.';
    backText = this.$translate.instant('closeButtonLabel');

    constructor(
        private $translate: ng.translate.ITranslateService,
        private AgentManager: AgentManager,
        private agentModal: AgentModal
    ) {}

    openAgentMissingDialog() {
        this.agentModal.agentDisconnected(this.backText, this.backState);
    }

    openNodeMissingDialog() {
        this.agentModal.clusterDisconnected(this.backText, this.backState);
    }
}
