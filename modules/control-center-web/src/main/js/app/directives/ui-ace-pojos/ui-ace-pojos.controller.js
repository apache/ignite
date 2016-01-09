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
    const ctrl = this;

    // Scope methods.
    $scope.onLoad = onLoad;

    // Watchers definition.
    // Watcher clean instance data if instance to cluster caches was change
    const cleanMetadatas = () => {
        delete ctrl.class;
        delete ctrl.metadatas;
        delete ctrl.classes;
    };

    // Watcher updata metadata when changes caches and checkers useConstructor and includeKeyFields
    const updateMetadatas = () => {
        delete ctrl.metadatas;

        if (!ctrl.cluster || !ctrl.cluster.caches)
            return;

        // TODO IGNITE-2054: need move $generatorJava to services.
        ctrl.metadatas = $generatorJava.pojos(ctrl.cluster.caches, ctrl.useConstructor, ctrl.includeKeyFields);
    };

    // Watcher update classes after
    const updateClasses = (value) => {
        delete ctrl.classes;

        if (!value)
            return;

        const classes = ctrl.classes = [];

        _.forEach(ctrl.metadatas, (meta) => {
            if (meta.keyType)
                classes.push(meta.keyType);

            classes.push(meta.valueType);
        });
    };

    // Update pojos class.
    const updateClass = (value) => {
        if (!value || !ctrl.metadatas.length)
            return;

        ctrl.class = ctrl.class || ctrl.metadatas[0].keyType || ctrl.metadatas[0].valueType;
    };

    // Update pojos data.
    const updatePojosData = (value) => {
        if (!value)
            return;

        _.forEach(ctrl.metadatas, (meta) => {
            if (meta.keyType === ctrl.class)
                return ctrl.data = meta.keyClass;

            if (meta.valueType === ctrl.class)
                return ctrl.data = meta.valueClass;
        });
    };

    // Setup watchers. Watchers order is important.
    $scope.$watch('ctrl.cluster.caches', cleanMetadatas);
    $scope.$watch('ctrl.cluster.caches', updateMetadatas);
    $scope.$watch('ctrl.cluster.caches', updateClasses);
    $scope.$watch('ctrl.useConstructor', updateMetadatas);
    $scope.$watch('ctrl.includeKeyFields', updateMetadatas);
    $scope.$watch('ctrl.metadatas', updateClass);
    $scope.$watch('ctrl.metadatas', updatePojosData);
    $scope.$watch('ctrl.class', updatePojosData);
}];
