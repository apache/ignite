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

const SNIPPET = 1;
const FACTORY = 2;

const SERVER_CFG = 'ServerConfigurationFactory';
const CLIENT_CFG = 'ClientConfigurationFactory';

const TYPES = [
    {value: SNIPPET, label: 'snippet'},
    {value: FACTORY, label: 'factory class'}
];

export default ['$scope', 'IgniteUiAceOnLoad', function($scope, onLoad) {
    var ctrl = this;

    // Scope values.
    $scope.type = SNIPPET;
    
    // Scope data.
    $scope.types = TYPES;
    
    // Scope methods.
    $scope.onLoad = onLoad;

    // watchers definition
    let clusterWatcher = (value) => {
        delete ctrl.data;

        if (!value)
            return;

        let type = $scope.type === FACTORY ? (!$scope.cfg ? SERVER_CFG : CLIENT_CFG) : false;

        // TODO IGNITE-2054: need move $generatorJava to services.
        ctrl.data = $generatorJava.cluster($scope.cluster, 'factory', type, $scope.cfg);
    };

    // Setup watchers. 
    $scope.$watch('cfg', clusterWatcher, true);
    $scope.$watch('type', clusterWatcher);
    $scope.$watch('cluster', clusterWatcher);
}]
