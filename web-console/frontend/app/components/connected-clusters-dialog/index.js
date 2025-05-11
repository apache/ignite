

import angular from 'angular';

import clusterSecurityIcon from '../cluster-security-icon';

import ConnectedClustersDialog from './service';

import connectedClustersList from './components/list';
import connectedClustersCellStatus from './components/cell-status';
import connectedClustersCellLogout from './components/cell-logout';

export default angular
    .module('ignite-console.connected-clusters-dialog', [
        clusterSecurityIcon.name
    ])
    .service('ConnectedClustersDialog', ConnectedClustersDialog)
    .component('connectedClustersList', connectedClustersList)
    .component('connectedClustersCellStatus', connectedClustersCellStatus)
    .component('connectedClustersCellLogout', connectedClustersCellLogout);
