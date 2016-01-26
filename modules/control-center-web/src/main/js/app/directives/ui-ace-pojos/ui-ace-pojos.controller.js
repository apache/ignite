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

export default ['$scope', 'IgniteUiAceOnLoad', 'JavaTypes', function($scope, onLoad, JavaTypes) {
    const ctrl = this;

    // Scope methods.
    $scope.onLoad = onLoad;

    // Watchers definition.
    // Watcher clean instance data if instance to cluster caches was change
    const cleanPojos = () => {
        delete ctrl.class;
        delete ctrl.pojos;
        delete ctrl.classes;
    };

    // Watcher update pojos when changes caches and checkers useConstructor and includeKeyFields
    const updatePojos = () => {
        delete ctrl.pojos;

        if (!ctrl.cluster || !ctrl.cluster.caches)
            return;

        // TODO IGNITE-2054: need move $generatorJava to services.
        ctrl.pojos = $generatorJava.pojos(ctrl.cluster.caches, ctrl.useConstructor, ctrl.includeKeyFields);
    };

    // Watcher update classes after
    const updateClasses = (value) => {
        delete ctrl.classes;

        if (!value)
            return;

        const classes = ctrl.classes = [];

        _.forEach(ctrl.pojos, (pojo) => {
            if (pojo.keyType && JavaTypes.nonBuiltInClass(pojo.keyType))
                classes.push(pojo.keyType);

            classes.push(pojo.valueType);
        });
    };

    // Update pojos class.
    const updateClass = (value) => {
        if (!value || !ctrl.pojos.length)
            return;

        const keyType = ctrl.pojos[0].keyType;

        ctrl.class = ctrl.class || (JavaTypes.nonBuiltInClass(keyType) ? keyType : null) || ctrl.pojos[0].valueType;
    };

    // Update pojos data.
    const updatePojosData = (value) => {
        if (!value)
            return;

        _.forEach(ctrl.pojos, (pojo) => {
            if (pojo.keyType === ctrl.class)
                return ctrl.data = pojo.keyClass;

            if (pojo.valueType === ctrl.class)
                return ctrl.data = pojo.valueClass;
        });
    };

    // Setup watchers. Watchers order is important.
    $scope.$watch('ctrl.cluster.caches', cleanPojos);
    $scope.$watch('ctrl.cluster.caches', updatePojos);
    $scope.$watch('ctrl.cluster.caches', updateClasses);
    $scope.$watch('ctrl.useConstructor', updatePojos);
    $scope.$watch('ctrl.includeKeyFields', updatePojos);
    $scope.$watch('ctrl.pojos', updateClass);
    $scope.$watch('ctrl.pojos', updatePojosData);
    $scope.$watch('ctrl.class', updatePojosData);
}];
