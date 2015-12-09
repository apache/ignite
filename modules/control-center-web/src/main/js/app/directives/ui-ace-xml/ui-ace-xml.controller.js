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

export default ['$scope', 'IgniteUiAceOnLoad', function($scope, onLoad) {
    var ctrl = this;

    // Scope methods.
    $scope.onLoad = onLoad;

    // Watchers definition.    
    let clusterWatcher = (value) => {
        delete ctrl.data;

        if (!value)
            return;

        // TODO IGNITE-2052: need move $generatorXml to services.
        ctrl.data = $generatorXml.cluster($scope.cluster, $scope.cfg)
    };

    // Setup watchers. 
    $scope.$watch('cfg', clusterWatcher, true);
    $scope.$watch('cluster', clusterWatcher);
}]
