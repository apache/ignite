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

export default ['$scope', 'JavaTypes', 'JavaTransformer', function($scope, JavaTypes, generator) {
    const ctrl = this;

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

        if (_.isNil(ctrl.cluster) || _.isEmpty(ctrl.cluster.caches))
            return;

        ctrl.pojos = generator.pojos(ctrl.cluster.caches, ctrl.useConstructor, ctrl.includeKeyFields);
    };

    // Watcher update classes after
    const updateClasses = (value) => {
        delete ctrl.classes;

        if (!value)
            return;

        const classes = ctrl.classes = [];

        _.forEach(ctrl.pojos, (pojo) => {
            if (_.nonNil(pojo.keyClass))
                classes.push(pojo.keyType);

            classes.push(pojo.valueType);
        });
    };

    // Update pojos class.
    const updateClass = (value) => {
        if (_.isEmpty(value))
            return;

        const pojo = value[0];

        ctrl.class = ctrl.class || (pojo.keyClass ? pojo.keyType : pojo.valueType);
    };

    // Update pojos data.
    const updatePojosData = (value) => {
        if (_.isNil(value))
            return;

        _.forEach(ctrl.pojos, (pojo) => {
            if (pojo.keyType === ctrl.class) {
                ctrl.data = pojo.keyClass;

                return false;
            }

            if (pojo.valueType === ctrl.class) {
                ctrl.data = pojo.valueClass;

                return false;
            }
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
