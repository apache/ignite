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

import {tap} from 'rxjs/operators';

export default class {
    static $inject = ['$scope', 'AgentManager', 'ConnectedClustersDialog'];

    connectedClusters = 0;

    /**
     * @param $scope Angular scope.
     * @param {import('app/modules/agent/AgentManager.service').default} agentMgr
     * @param {import('../connected-clusters-dialog/service').default} connectedClustersDialog
     */
    constructor($scope, agentMgr, connectedClustersDialog) {
        this.$scope = $scope;
        this.agentMgr = agentMgr;
        this.connectedClustersDialog = connectedClustersDialog;
    }

    show() {
        this.connectedClustersDialog.show({
            clusters: this.clusters
        });
    }

    $onInit() {
        this.connectedClusters$ = this.agentMgr.connectionSbj.pipe(
            tap(({ clusters }) => this.connectedClusters = clusters.length),
            tap(({ clusters }) => {
                this.clusters = clusters;
                this.$scope.$applyAsync();
            })
        )
        .subscribe();
    }

    $onDestroy() {
        this.connectedClusters$.unsubscribe();
    }
}
