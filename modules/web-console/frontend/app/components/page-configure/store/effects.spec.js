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

import {assert} from 'chai';
import {of} from 'rxjs/observable/of';
import {_throw} from 'rxjs/observable/throw';
import {default as Effects} from './effects';
import {default as Selectors} from './selectors';
import {TestScheduler} from 'rxjs/testing/TestScheduler';

const makeMocks = (target, mocks) => new Map(target.$inject.map((provider) => {
    return (provider in mocks) ? [provider, mocks[provider]] : [provider, {}];
}));

suite('Configuration store effects', () => {
    suite('Load and edit cluster', () => {
        const actionValues = {
            a: {type: 'LOAD_AND_EDIT_CLUSTER', clusterID: 'new'},
            b: {type: 'LOAD_AND_EDIT_CLUSTER', clusterID: '1'},
            c: {type: 'LOAD_AND_EDIT_CLUSTER', clusterID: '2'}
        };

        const stateValues = {
            A: {
                shortClusters: {value: new Map()},
                clusters: new Map()
            },
            B: {
                shortClusters: {value: new Map([['1', {id: '1', name: 'Cluster'}]])},
                clusters: new Map([['1', {id: '1', name: 'Cluster'}]])
            }
        };

        const setup = ({actionMarbles, stateMarbles, mocks}) => {
            const testScheduler = new TestScheduler((actual, expected) => assert.deepEqual(actual, expected));
            const mocksMap = makeMocks(Effects, {
                ...mocks,
                ConfigureState: (() => {
                    const actions$ = testScheduler.createHotObservable(actionMarbles, actionValues);
                    const state$ = testScheduler.createHotObservable(stateMarbles, stateValues);
                    return {actions$, state$};
                })()
            });

            const effects = new Effects(...mocksMap.values());

            return {testScheduler, effects};
        };

        const mocks = {
            Clusters: {
                getBlankCluster: () => ({id: 'foo'}),
                getCluster: (id) => of({data: {id}})
            },
            ConfigSelectors: new Selectors()
        };

        test('New cluster', () => {
            const {testScheduler, effects} = setup({
                actionMarbles: '-a',
                stateMarbles: 'B-',
                mocks
            });

            testScheduler.expectObservable(effects.loadAndEditClusterEffect$).toBe('-(ab)', {
                a: {type: 'EDIT_CLUSTER', cluster: {id: 'foo', name: 'Cluster1'}},
                b: {type: 'LOAD_AND_EDIT_CLUSTER_OK'}
            });

            testScheduler.flush();
        });

        test('Cached cluster', () => {
            const {testScheduler, effects} = setup({
                actionMarbles: '-b',
                stateMarbles: 'AB',
                mocks
            });

            testScheduler.expectObservable(effects.loadAndEditClusterEffect$).toBe('-(ab)', {
                a: {type: 'EDIT_CLUSTER', cluster: {id: '1', name: 'Cluster'}},
                b: {type: 'LOAD_AND_EDIT_CLUSTER_OK'}
            });

            testScheduler.flush();
        });

        test('Cluster from server, success', () => {
            const {testScheduler, effects} = setup({
                actionMarbles: '-c',
                stateMarbles: 'AB',
                mocks
            });

            testScheduler.expectObservable(effects.loadAndEditClusterEffect$).toBe('-(abc)', {
                a: {type: 'UPSERT_CLUSTERS', items: [{id: '2'}]},
                b: {type: 'EDIT_CLUSTER', cluster: {id: '2'}},
                c: {type: 'LOAD_AND_EDIT_CLUSTER_OK'}
            });

            testScheduler.flush();
        });

        test('Cluster from server, error', () => {
            const {testScheduler, effects} = setup({
                actionMarbles: '-c',
                stateMarbles: 'AB',
                mocks: {
                    ...mocks,
                    Clusters: {getCluster: () => _throw({data: 'Error'})}
                }
            });

            testScheduler.expectObservable(effects.loadAndEditClusterEffect$).toBe('-a', {
                a: {type: 'LOAD_AND_EDIT_CLUSTER_ERR', error: {message: `Failed to load cluster: Error.`}}
            });

            testScheduler.flush();
        });
    });
});
