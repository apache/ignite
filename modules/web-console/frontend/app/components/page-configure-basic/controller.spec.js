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

import {suite, test} from 'mocha';
import {assert} from 'chai';
import {spy} from 'sinon';

import {Subject} from 'rxjs/Subject';
import Controller from './controller';

const mocks = () => new Map([
    ['$scope', {
        $applyAsync: spy(function() {
            this._fn = fn;
        })
    }],
    ['pageService', {
        setCluster: spy()
    }],
    ['Clusters', {
        discoveries: 1,
        minMemoryPolicySize: 1000
    }],
    ['ConfigureState', {
        state$: new Subject()
    }],
    ['ConfigurationDownload', {
        downloadClusterConfiguration: spy()
    }],
    ['IgniteVersion', {
        currentSbj: new BehaviorSubject({ignite: '1.9.0'})
    }]
]);

suite('page-configure-basic component controller', () => {
    test('$onInit', () => {
        const c = new Controller(...mocks().values());
        c.$onInit();
        assert.equal(c.discoveries, 1, 'exposes discoveries');
        assert.equal(c.minMemorySize, 1000, 'exposes minMemorySize');
        assert.deepEqual(
            c.sizesMenu,
            [
                {label: 'Kb', value: 1024},
                {label: 'Mb', value: 1024 * 1024},
                {label: 'Gb', value: 1024 * 1024 * 1024}
            ],
            'exposes sizesMenu'
        );
        assert.equal(c.memorySizeScale, c.sizesMenu[2], 'sets default memorySizeScale to Gb');
        assert.deepEqual(c.pageService.setCluster.getCall(0).args, [-1], 'sets cluster to -1');
    });
    // TODO IGNITE-5271 Cover rest of controller features with tests
    test.skip('State subscription', () => {
        const c = new Controller(...mocks().values());
        c.$onInit();
        const state = {
            list: {
                clusters: new Map([
                    [1, {_id: 1, name: '1'}],
                    [2, {_id: 2, name: '2'}]
                ]),
                caches: new Map([
                    [1, {_id: 1, name: '1'}],
                    [2, {_id: 2, name: '2'}]
                ])
            },
            configureBasic: {
                newClusterCaches: [1, 2, 3],
                oldClusterCaches: [4, 5, 6],
                cluster: {
                    memoryConfiguration: {
                        memoryPolicies: [{name: 'Default'}]
                    }
                }
            }
        };
        c.ConfigureState.state$.next(state);
        assert.equal(c.clusters, state.list.clusters, 'gets clusters from state');
    });
});
