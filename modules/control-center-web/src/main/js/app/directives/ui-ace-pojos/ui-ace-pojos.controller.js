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

    // scope values

    // scope data
    
    // scope methods

    // watchers definition
    let metadatasWatcher = (value) => {
        delete ctrl.data;

        if (!value) {
            return;
        }
     
        // TODO IGNITE-2054: need move $generatorJava to services
        var ctrl.metadatas = $generatorJava.pojos($scope.cluster.caches, $scope.useConstructor, $scope.includeKeyFields);
    }

    let classesWatcher = (value) => {
        delete ctrl.classes;

        if (!value) {
            return;
        }

        let classes = ctrl.classes = []

        _.forEach(ctrl.metadatas, (meta) => {
            classes.push(meta.keyType);
            classes.push(meta.valueType);
        });
    };

    // watches
    $scope.$watch('cluster.caches', metadatasWatcher);
    $scope.$watch('ctrl.metadatas', classesWatcher);
    $scope.$watch('ctrl.metadatas', )
}]