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

import {TestScheduler} from 'rxjs/testing/TestScheduler';
import {Subject} from 'rxjs/Subject';
import {BehaviorSubject} from 'rxjs/BehaviorSubject';
import {Subscriber} from 'rxjs/Subscriber';
import Controller from './controller';

const mocks = () => new Map([
    ['$scope', {
        $applyAsync: spy((fn) => fn())
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
        currentSbj: new BehaviorSubject({ignite: '1.9.0'}),
        since: (a, b) => a === b
    }],
    ['state$', {
        params: {
            clusterID: null
        }
    }]
]);

suite.skip('page-configure-basic component controller', () => {
    test('$onInit method', () => {
        const c = new Controller(...mocks().values());
        c.getObservable = spy(c.getObservable.bind(c));
        c.$onInit();
        assert.deepEqual(
            c.getObservable.lastCall.args,
            [c.ConfigureState.state$, c.Version.currentSbj],
            'calls getObservable with correct arguments'
        );
        assert.instanceOf(c.subscription, Subscriber, 'stores subscription for later');
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
        assert.deepEqual(
            c.pageService.setCluster.lastCall.args, ['-1'],
            'sets cluster to -1 by clusterID state param is missing'
        );
    });

    test('$onDestroy method', () => {
        const c = new Controller(...mocks().values());
        c.$onInit();
        c.subscription.unsubscribe = spy(c.subscription.unsubscribe);
        c.$onDestroy();
        assert(c.subscription.unsubscribe.calledOnce, 'unsubscribes from Observable');
    });

    test('getObservable method', () => {
        const testScheduler = new TestScheduler((...args) => assert.deepEqual(...args));
        const c = new Controller(...mocks().values());

        c.applyValue = spy(c.applyValue.bind(c));

        const version  = 'a-b-';
        const state    = '-a-b';
        const expected = '-abc';

        const version$ = testScheduler.createHotObservable(version, {
            a: {ignite: '1.9.0'},
            b: {ignite: '2.0.0'}
        });

        const state$ = testScheduler.createHotObservable(state, {
            a: {
                list: {
                    clusters: new Map(),
                    caches: new Map()
                },
                configureBasic: {
                    newClusterCaches: [],
                    oldClusterCaches: [],
                    cluster: null
                }
            },
            b: {
                list: {
                    clusters: new Map([
                        [1, {_id: 1, name: '1', caches: [1, 2]}],
                        [2, {_id: 2, name: '2'}]
                    ]),
                    caches: new Map([
                        [1, {_id: 1, name: '1'}],
                        [2, {_id: 2, name: '2'}]
                    ])
                },
                configureBasic: {
                    newClusterCaches: [],
                    oldClusterCaches: [
                        {_id: 1, name: '1'},
                        {_id: 2, name: '2'}
                    ],
                    cluster: {_id: 1, name: '1', caches: [1, 2]}
                }
            }
        });


        const expectedValues = {
            a: {
                clusters: new Map(),
                caches: new Map(),
                state: {
                    newClusterCaches: [],
                    oldClusterCaches: [],
                    cluster: null
                },
                allClusterCaches: [],
                cachesMenu: [],
                defaultMemoryPolicy: void 0,
                memorySizeInputVisible: false
            },
            b: {
                clusters: new Map(),
                caches: new Map(),
                state: {
                    newClusterCaches: [],
                    oldClusterCaches: [],
                    cluster: null
                },
                allClusterCaches: [],
                cachesMenu: [],
                defaultMemoryPolicy: void 0,
                memorySizeInputVisible: true
            },
            c: {
                clusters: new Map([
                    [1, {_id: 1, name: '1', caches: [1, 2]}],
                    [2, {_id: 2, name: '2'}]
                ]),
                caches: new Map([
                    [1, {_id: 1, name: '1'}],
                    [2, {_id: 2, name: '2'}]
                ]),
                state: {
                    newClusterCaches: [],
                    oldClusterCaches: [
                        {_id: 1, name: '1'},
                        {_id: 2, name: '2'}
                    ],
                    cluster: {_id: 1, name: '1', caches: [1, 2]}
                },
                allClusterCaches: [
                    {_id: 1, name: '1'},
                    {_id: 2, name: '2'}
                ],
                cachesMenu: [
                    {_id: 1, name: '1'},
                    {_id: 2, name: '2'}
                ],
                defaultMemoryPolicy: void 0,
                memorySizeInputVisible: true
            }
        };

        testScheduler.expectObservable(c.getObservable(state$, version$)).toBe(expected, expectedValues);
        testScheduler.flush();

        assert.deepEqual(c.applyValue.getCall(0).args[0], expectedValues.a, 'applies value a');
        assert.deepEqual(c.applyValue.getCall(1).args[0], expectedValues.b, 'applies value b');
        assert.deepEqual(c.applyValue.getCall(2).args[0], expectedValues.c, 'applies value c');
        assert.equal(c.applyValue.callCount, 3, 'applyValue was called correct amount of times');
    });
});
