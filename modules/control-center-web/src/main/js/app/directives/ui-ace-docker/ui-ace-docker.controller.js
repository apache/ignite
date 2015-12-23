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

import types from 'app/data/os.json!';

export default ['$scope', 'IgniteUiAceOnLoad', function($scope, onLoad) {
    const ctrl = this;

    // Scope values.
    $scope.types = types;

    // Scope methods.
    $scope.onLoad = onLoad;

    // Watchers definition.
    const clusterWatcher = () => {
        delete ctrl.data;

        if (!$scope.cluster)
            return;

        // TODO IGNITE-2058: need move $generatorDocker to services.
        ctrl.data = $generatorDocker.clusterDocker($scope.cluster, $scope.type);
    };

    // Setup watchers.
    $scope.$watch('type', clusterWatcher);
    $scope.$watch('cluster', clusterWatcher);
}];
