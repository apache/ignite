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

import _ from 'lodash';

export default class {
    static $inject = ['$scope', 'AgentManager', 'IgniteConfirm'];

    constructor($scope, agentMgr, Confirm) {
        Object.assign(this, { $scope, agentMgr, Confirm });

        this.clusters = [];
        this.isDemo = agentMgr.isDemoMode();
    }

    $onInit() {
        this.clusters$ = this.agentMgr.connectionSbj
            .do(({ cluster, clusters }) => {
                this.cluster = cluster;
                this.clusters = _.orderBy(clusters, ['name'], ['asc']);
            })
            .subscribe(() => {});
    }

    $onDestroy() {
        this.clusters$.unsubscribe();
    }

    change() {
        this.agentMgr.switchCluster(this.cluster);
    }

    toggle($event) {
        $event.preventDefault();

        const toggleClusterState = () => {
            this.inProgress = true;

            return this.agentMgr.toggleClusterState()
                .finally(() => this.inProgress = false);
        };

        if (this.cluster.active) {
            return this.Confirm.confirm('Are you sure you want to deactivate cluster?')
                .then(() => toggleClusterState());
        }

        return toggleClusterState();
    }
}
