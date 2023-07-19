

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
