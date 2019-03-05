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

import 'mocha';
import {assert} from 'chai';
import {spy} from 'sinon';
import Controller from './controller';

suite('cluster-edit-form controller', () => {
    test('cluster binding changes', () => {
        const $scope = {
            ui: {
                inputForm: {
                    $setPristine: spy(),
                    $setUntouched: spy()
                }
            }
        };

        const mocks = Controller.$inject.map((token) => {
            switch (token) {
                case '$scope': return $scope;
                default: return null;
            }
        });

        const changeBoundCluster = ($ctrl, cluster) => {
            $ctrl.cluster = cluster;
            $ctrl.$onChanges({
                cluster: {
                    currentValue: cluster
                }
            });
        };

        const $ctrl = new Controller(...mocks);

        const cluster1 = {_id: 1, caches: [1, 2, 3]};
        const cluster2 = {_id: 1, caches: [1, 2, 3, 4, 5, 6], models: [1, 2, 3]};
        const cluster3 = {_id: 1, caches: [1, 2, 3, 4, 5, 6], models: [1, 2, 3], name: 'Foo'};

        changeBoundCluster($ctrl, cluster1);

        assert.notEqual($ctrl.clonedCluster, cluster1, 'Cloned cluster is really cloned');
        assert.deepEqual($ctrl.clonedCluster, cluster1, 'Cloned cluster is really a clone of incloming value');
        assert.equal(1, $scope.ui.inputForm.$setPristine.callCount, 'Sets form pristine when cluster value changes');
        assert.equal(1, $scope.ui.inputForm.$setUntouched.callCount, 'Sets form untouched when cluster value changes');

        changeBoundCluster($ctrl, cluster2);

        assert.deepEqual(
            $ctrl.clonedCluster,
            cluster2,
            'Overrides clonedCluster if incoming cluster has same id but different caches or models'
        );
        assert.equal(2, $scope.ui.inputForm.$setPristine.callCount, 'Sets form pristine when bound cluster caches/models change');
        assert.equal(2, $scope.ui.inputForm.$setUntouched.callCount, 'Sets form untouched when bound cluster caches/models change');

        changeBoundCluster($ctrl, cluster3);

        assert.deepEqual(
            $ctrl.clonedCluster,
            cluster2,
            'Does not change cloned cluster value if fields other than id, chaches and models change'
        );
    });
});
