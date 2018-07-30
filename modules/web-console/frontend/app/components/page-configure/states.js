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

import pageConfigureAdvancedClusterComponent from '../page-configure-advanced/components/page-configure-advanced-cluster/component';
import pageConfigureAdvancedModelsComponent from '../page-configure-advanced/components/page-configure-advanced-models/component';
import pageConfigureAdvancedCachesComponent from '../page-configure-advanced/components/page-configure-advanced-caches/component';
import pageConfigureAdvancedIGFSComponent from '../page-configure-advanced/components/page-configure-advanced-igfs/component';

import {Observable} from 'rxjs/Observable';

const idRegex = `new|[a-z0-9]+`;

const shortCachesResolve = ['ConfigSelectors', 'ConfigureState', 'ConfigEffects', '$transition$', (ConfigSelectors, ConfigureState, {etp}, $transition$) => {
    if ($transition$.params().clusterID === 'new') return Promise.resolve();
    return Observable.fromPromise($transition$.injector().getAsync('_cluster'))
    .switchMap(() => ConfigureState.state$.let(ConfigSelectors.selectCluster($transition$.params().clusterID)).take(1))
    .switchMap((cluster) => {
        return etp('LOAD_SHORT_CACHES', {ids: cluster.caches, clusterID: cluster._id});
    })
    .toPromise();
}];

function registerStates($stateProvider) {
    // Setup the states.
    $stateProvider
    .state('base.configuration', {
        abstract: true,
        permission: 'configuration',
        url: '/configuration',
        onEnter: ['ConfigureState', (ConfigureState) => ConfigureState.dispatchAction({type: 'PRELOAD_STATE', state: {}})],
        resolve: {
            _shortClusters: ['ConfigEffects', ({etp}) => {
                return etp('LOAD_USER_CLUSTERS');
            }]
        },
        resolvePolicy: {
            async: 'NOWAIT'
        }
    })
    .state('base.configuration.overview', {
        url: '/overview',
        component: 'pageConfigureOverview',
        permission: 'configuration',
        tfMetaTags: {
            title: 'Configuration'
        }
    })
    .state('base.configuration.edit', {
        url: `/{clusterID:${idRegex}}`,
        permission: 'configuration',
        component: 'pageConfigure',
        resolve: {
            _cluster: ['ConfigEffects', '$transition$', ({etp}, $transition$) => {
                return $transition$.injector().getAsync('_shortClusters').then(() => {
                    return etp('LOAD_AND_EDIT_CLUSTER', {clusterID: $transition$.params().clusterID});
                });
            }]
        },
        data: {
            errorState: 'base.configuration.overview'
        },
        redirectTo: ($transition$) => {
            const [ConfigureState, ConfigSelectors] = ['ConfigureState', 'ConfigSelectors'].map((t) => $transition$.injector().get(t));
            const waitFor = ['_cluster', '_shortClusters'].map((t) => $transition$.injector().getAsync(t));
            return Observable.fromPromise(Promise.all(waitFor)).switchMap(() => {
                return Observable.combineLatest(
                    ConfigureState.state$.let(ConfigSelectors.selectCluster($transition$.params().clusterID)).take(1),
                    ConfigureState.state$.let(ConfigSelectors.selectShortClusters()).take(1)
                );
            })
            .map(([cluster = {caches: []}, clusters]) => {
                return (clusters.value.size > 10 || cluster.caches.length > 5)
                    ? 'base.configuration.edit.advanced'
                    : 'base.configuration.edit.basic';
            })
            .toPromise();
        },
        failState: 'signin',
        tfMetaTags: {
            title: 'Configuration'
        }
    })
    .state('base.configuration.edit.basic', {
        url: '/basic',
        component: 'pageConfigureBasic',
        permission: 'configuration',
        resolve: {
            _shortCaches: shortCachesResolve
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        tfMetaTags: {
            title: 'Basic Configuration'
        }
    })
    .state('base.configuration.edit.advanced', {
        url: '/advanced',
        component: 'pageConfigureAdvanced',
        permission: 'configuration',
        redirectTo: 'base.configuration.edit.advanced.cluster'
    })
    .state('base.configuration.edit.advanced.cluster', {
        url: '/cluster',
        component: pageConfigureAdvancedClusterComponent.name,
        permission: 'configuration',
        resolve: {
            _shortCaches: shortCachesResolve
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        tfMetaTags: {
            title: 'Configure Cluster'
        }
    })
    .state('base.configuration.edit.advanced.caches', {
        url: '/caches',
        permission: 'configuration',
        component: pageConfigureAdvancedCachesComponent.name,
        resolve: {
            _shortCachesAndModels: ['ConfigSelectors', 'ConfigureState', 'ConfigEffects', '$transition$', (ConfigSelectors, ConfigureState, {etp}, $transition$) => {
                if ($transition$.params().clusterID === 'new') return Promise.resolve();
                return Observable.fromPromise($transition$.injector().getAsync('_cluster'))
                .switchMap(() => ConfigureState.state$.let(ConfigSelectors.selectCluster($transition$.params().clusterID)).take(1))
                .map((cluster) => {
                    return Promise.all([
                        etp('LOAD_SHORT_CACHES', {ids: cluster.caches, clusterID: cluster._id}),
                        etp('LOAD_SHORT_MODELS', {ids: cluster.models, clusterID: cluster._id}),
                        etp('LOAD_SHORT_IGFSS', {ids: cluster.igfss, clusterID: cluster._id})
                    ]);
                })
                .toPromise();
            }]
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        tfMetaTags: {
            title: 'Configure Caches'
        }
    })
    .state('base.configuration.edit.advanced.caches.cache', {
        url: `/{cacheID:${idRegex}}`,
        permission: 'configuration',
        resolve: {
            _cache: ['ConfigEffects', '$transition$', ({etp}, $transition$) => {
                const {clusterID, cacheID} = $transition$.params();
                if (cacheID === 'new') return Promise.resolve();
                return etp('LOAD_CACHE', {cacheID});
            }]
        },
        data: {
            errorState: 'base.configuration.edit.advanced.caches'
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        tfMetaTags: {
            title: 'Configure Caches'
        }
    })
    .state('base.configuration.edit.advanced.models', {
        url: '/models',
        component: pageConfigureAdvancedModelsComponent.name,
        permission: 'configuration',
        resolve: {
            _shortCachesAndModels: ['ConfigSelectors', 'ConfigureState', 'ConfigEffects', '$transition$', (ConfigSelectors, ConfigureState, {etp}, $transition$) => {
                if ($transition$.params().clusterID === 'new') return Promise.resolve();
                return Observable.fromPromise($transition$.injector().getAsync('_cluster'))
                .switchMap(() => ConfigureState.state$.let(ConfigSelectors.selectCluster($transition$.params().clusterID)).take(1))
                .map((cluster) => {
                    return Promise.all([
                        etp('LOAD_SHORT_CACHES', {ids: cluster.caches, clusterID: cluster._id}),
                        etp('LOAD_SHORT_MODELS', {ids: cluster.models, clusterID: cluster._id})
                    ]);
                })
                .toPromise();
            }]
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        tfMetaTags: {
            title: 'Configure SQL Schemes'
        }
    })
    .state('base.configuration.edit.advanced.models.model', {
        url: `/{modelID:${idRegex}}`,
        resolve: {
            _cache: ['ConfigEffects', '$transition$', ({etp}, $transition$) => {
                const {clusterID, modelID} = $transition$.params();
                if (modelID === 'new') return Promise.resolve();
                return etp('LOAD_MODEL', {modelID});
            }]
        },
        data: {
            errorState: 'base.configuration.edit.advanced.models'
        },
        permission: 'configuration',
        resolvePolicy: {
            async: 'NOWAIT'
        }
    })
    .state('base.configuration.edit.advanced.igfs', {
        url: '/igfs',
        component: pageConfigureAdvancedIGFSComponent.name,
        permission: 'configuration',
        resolve: {
            _shortIGFSs: ['ConfigSelectors', 'ConfigureState', 'ConfigEffects', '$transition$', (ConfigSelectors, ConfigureState, {etp}, $transition$) => {
                if ($transition$.params().clusterID === 'new') return Promise.resolve();
                return Observable.fromPromise($transition$.injector().getAsync('_cluster'))
                .switchMap(() => ConfigureState.state$.let(ConfigSelectors.selectCluster($transition$.params().clusterID)).take(1))
                .map((cluster) => {
                    return Promise.all([
                        etp('LOAD_SHORT_IGFSS', {ids: cluster.igfss, clusterID: cluster._id})
                    ]);
                })
                .toPromise();
            }]
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        tfMetaTags: {
            title: 'Configure IGFS'
        }
    })
    .state('base.configuration.edit.advanced.igfs.igfs', {
        url: `/{igfsID:${idRegex}}`,
        permission: 'configuration',
        resolve: {
            _igfs: ['ConfigEffects', '$transition$', ({etp}, $transition$) => {
                const {clusterID, igfsID} = $transition$.params();
                if (igfsID === 'new') return Promise.resolve();
                return etp('LOAD_IGFS', {igfsID});
            }]
        },
        data: {
            errorState: 'base.configuration.edit.advanced.igfs'
        },
        resolvePolicy: {
            async: 'NOWAIT'
        }
    });
}

registerStates.$inject = ['$stateProvider'];

export {registerStates};
