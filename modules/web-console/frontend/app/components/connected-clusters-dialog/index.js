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
