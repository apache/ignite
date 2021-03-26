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

import { BehaviorSubject } from 'rxjs';
import {tap, filter, combineLatest} from 'rxjs/operators';
import {CancellationError} from 'app/errors/CancellationError';

export default class {
    clusters = [];
    isDemo = this.agentMgr.isDemoMode();
    _inProgressSubject = new BehaviorSubject(false);

    static $inject = ['AgentManager', 'IgniteConfirm', 'IgniteVersion', 'IgniteMessages', '$translate'];

    constructor(
        private agentMgr,
        private Confirm,
        private Version,
        private Messages,
        private $translate: ng.translate.ITranslateService
    ) {}

    $onInit() {
        if (this.isDemo)
            return;

        this.inProgress$ = this._inProgressSubject.asObservable();

        this.clusters$ = this.agentMgr.connectionSbj.pipe(
            combineLatest(this.inProgress$),
            tap(([sbj, inProgress]) => this.inProgress = inProgress),
            filter(([sbj, inProgress]) => !inProgress),
            tap(([{cluster, clusters}]) => {
                this.cluster = cluster ? {...cluster} : null;
                this.clusters = _.orderBy(clusters, ['name'], ['asc']);
            })
        )
        .subscribe(() => {});
    }

    $onDestroy() {
        if (!this.isDemo)
            this.clusters$.unsubscribe();
    }

    change(item) {
        this.agentMgr.switchCluster(item)
            .then(() => this.cluster = item)
            .catch((err) => {
                if (!(err instanceof CancellationError))
                    this.Messages.showError(this.$translate.instant('clusterSelectorComponent.failedToSwitchClusterErrorMessage'), err);
            });
    }

    isChangeStateAvailable() {
        return !this.isDemo && this.cluster && this.Version.since(this.cluster.clusterVersion, '2.0.0');
    }

    toggle($event) {
        $event.preventDefault();

        const toggleClusterState = () => {
            this._inProgressSubject.next(true);

            return this.agentMgr.toggleClusterState()
                .then(() => this._inProgressSubject.next(false))
                .catch((err) => {
                    this._inProgressSubject.next(false);

                    this.Messages.showError(this.$translate.instant('clusterSelectorComponent.failedToToggleClusterStateErrorMessage'), err);
                });
        };

        if (this.cluster.active) {
            return this.Confirm.confirm(this.$translate.instant('clusterSelectorComponent.deactivateConfirmationMessage'))
                .then(() => toggleClusterState());
        }

        return toggleClusterState();
    }
}
