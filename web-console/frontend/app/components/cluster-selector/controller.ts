

import _ from 'lodash';

import { BehaviorSubject } from 'rxjs';
import {tap, filter, combineLatest} from 'rxjs/operators';
import {CancellationError} from 'app/errors/CancellationError';

export default class {
    clusters = [];
    isDemo = this.agentMgr.isDemoMode();
    _inProgressSubject = new BehaviorSubject(false);
    clusterId = null;
    cluster: any = null;
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
                if(cluster && localStorage.clusterId && localStorage.clusterId===cluster.id){
                    this.cluster = {...cluster}
                    // add@byron
                    localStorage.clusterName = cluster.name;
                    // end@
                }
                else if(!this.cluster){
                    this.cluster = cluster ? {...cluster} : null;
                }                
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
            .then(() => {
                this.cluster = item;
                // add@byron
                localStorage.clusterName = item.name;
                // end@
            })
            .catch((err) => {
                if (!(err instanceof CancellationError))
                    this.Messages.showError(this.$translate.instant('clusterSelectorComponent.failedToSwitchClusterErrorMessage'), err);
            });
    }

    isChangeStateAvailable() {
        return !this.isDemo && this.cluster;
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
