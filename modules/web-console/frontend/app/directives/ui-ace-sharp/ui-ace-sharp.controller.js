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

const SERVER_CFG = 'ServerConfigurationFactory';
const CLIENT_CFG = 'ClientConfigurationFactory';

/**
 * @param {ng.IScope} $scope
 * @param {import('app/modules/configuration/generator/SharpTransformer.service').default} generator
 */
export default function controller($scope, generator) {
    /** @type {ThisType} */
    const ctrl = this;

    this.$onInit = () => {
        delete ctrl.data;

        // Set default generator
        ctrl.generator = (cluster) => {
            const type = $scope.cfg ? CLIENT_CFG : SERVER_CFG;

            return generator.cluster(cluster, 'config', type, $scope.cfg);
        };
    };
}

controller.$inject = ['$scope', 'IgniteSharpTransformer'];
