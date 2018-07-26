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

import { BehaviorSubject } from 'rxjs/BehaviorSubject';
import 'rxjs/add/operator/combineLatest';

export default class {
    static $inject = ['AgentManager', 'IgniteConfirm'];

    /**
     * @param agentMgr Agent manager.
     * @param Confirm  Confirmation service.
     */
    constructor(agentMgr, Confirm) {
        this.agentMgr = agentMgr;
        this.Confirm = Confirm;
        this.clusters = [];
        this.isDemo = agentMgr.isDemoMode();
        this._inProgressSubject = new BehaviorSubject(false);
    }

    $onInit() {
        if (this.isDemo)
            return;

        this.inProgress$ = this._inProgressSubject.asObservable();

        this.clusters$ = this.agentMgr.connectionSbj
            .combineLatest(this.inProgress$)
            .do(([sbj, inProgress]) => this.inProgress = inProgress)
            .filter(([sbj, inProgress]) => !inProgress)
            .do(([{cluster, clusters}]) => {
                this.cluster = cluster ? {...cluster} : null;
                this.clusters = _.orderBy(clusters, ['name'], ['asc']);
            })
            .subscribe(() => {});
    }

    $onDestroy() {
        if (!this.isDemo)
            this.clusters$.unsubscribe();
    }

    change() {
        this.agentMgr.switchCluster(this.cluster);
    }

    toggle($event) {
        $event.preventDefault();

        const toggleClusterState = () => {
            this._inProgressSubject.next(true);

            // IGNITE-8744 For some reason .finally() not working in Firefox, needed to be investigated later.
            return this.agentMgr.toggleClusterState()
                .then(() => this._inProgressSubject.next(false))
                .catch(() => this._inProgressSubject.next(false));
        };

        if (this.cluster.active) {
            return this.Confirm.confirm('Are you sure you want to deactivate cluster?')
                .then(() => toggleClusterState());
        }

        return toggleClusterState();
    }
}
