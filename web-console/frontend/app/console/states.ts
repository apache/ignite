import {StateParams} from '@uirouter/angularjs';

import pageConsoleAdvancedClusterComponent from './components/page-configure-advanced/components/page-configure-advanced-cluster/component';
import pageConsoleAdvancedModelsComponent from './components/page-configure-advanced/components/page-configure-advanced-models/component';
import pageConsoleAdvancedCachesComponent from './components/page-configure-advanced/components/page-configure-advanced-caches/component';

import {from, combineLatest} from 'rxjs';
import {switchMap, take, map} from 'rxjs/operators';

export type ClusterParams = ({clusterID: string} | {clusterID: 'new'}) & StateParams;

const shortCachesResolve = ['ConfigSelectors', 'ConfigureState', 'ConfigEffects', '$transition$', function(ConfigSelectors, ConfigureState, {etp}, $transition$) {
    if ($transition$.params().clusterID === 'new')
        return Promise.resolve();
    return from($transition$.injector().getAsync('_cluster')).pipe(
        switchMap(() => ConfigureState.state$.pipe(ConfigSelectors.selectCluster($transition$.params().clusterID), take(1))),
        switchMap((cluster) => {
            return etp('LOAD_SHORT_CACHES', {ids: cluster.caches, clusterID: cluster.id});
        })
    )
    .toPromise();
}];

const shortCachesAndModels = ['ConfigSelectors', 'ConfigureState', 'ConfigEffects', '$transition$', (ConfigSelectors, ConfigureState, {etp}, $transition$) => {
    if ($transition$.params().clusterID === 'new')
        return Promise.resolve();

    return from($transition$.injector().getAsync('_cluster')).pipe(
        switchMap(() => ConfigureState.state$.pipe(ConfigSelectors.selectCluster($transition$.params().clusterID), take(1))),
        map((cluster) => {
            return Promise.all([
                etp('LOAD_SHORT_CACHES', {ids: cluster.caches, clusterID: cluster.id}),
                etp('LOAD_SHORT_MODELS', {ids: cluster.models, clusterID: cluster.id})
            ]);
        })
    ).toPromise();
}]

function registerStates($stateProvider) {
    // Setup the states.
  $stateProvider
    .state('base.console', {
        permission: 'management',
        url: '/console',
        onEnter: ['ConfigureState', (ConfigureState) => ConfigureState.dispatchAction({type: 'PRELOAD_STATE', state: {}})],
        resolve: {
            _shortClusters: ['ConfigEffects', ({etp}) => {
                return etp('LOAD_USER_CLUSTERS');
            }]
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        redirectTo: 'base.console.overview'
    })
    .state('base.console.overview', {
        url: '/overview',
        component: 'pageConsoleOverview',
        permission: 'management',
        tfMetaTags: {
            title: 'Console'
        }
    })
    .state('base.console.edit', {
        url: `/{clusterID}`,
        permission: 'management',
        component: 'pageConsole',
        resolve: {
            _cluster: ['ConfigEffects', '$transition$', ({etp}, $transition$) => {
                return $transition$.injector().getAsync('_shortClusters').then(() => {
                    return etp('LOAD_AND_EDIT_CLUSTER', {clusterID: $transition$.params().clusterID});
                });
            }]
        },
        data: {
            errorState: 'base.console.overview'
        },
        redirectTo: ($transition$) => {
            const [ConfigureState, ConfigSelectors] = ['ConfigureState', 'ConfigSelectors'].map((t) => $transition$.injector().get(t));
            const waitFor = ['_cluster', '_shortClusters'].map((t) => $transition$.injector().getAsync(t));
            return from(Promise.all(waitFor)).pipe(
                switchMap(() => {
                    return combineLatest(
                        ConfigureState.state$.pipe(ConfigSelectors.selectCluster($transition$.params().clusterID), take(1)),
                        ConfigureState.state$.pipe(ConfigSelectors.selectShortClusters(), take(1))
                    );
                }),
                map(([cluster = {caches: []}, clusters]) => {
                    return 'base.console.edit.basic'
                })
            )
            .toPromise();
        },
        failState: 'signin',
        tfMetaTags: {
            title: 'Console'
        }
    })
    .state('base.console.edit.basic', {
        url: '/basic',
        component: 'pageConsoleBasic',
        permission: 'management',
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
    .state('base.console.edit.service', {
        url: '/service',
        component: 'pageConsoleService',
        permission: 'management',
        resolve: {
            _shortCachesAndModels: shortCachesAndModels,
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        tfMetaTags: {
            title: 'Grid Service Console'
        }
    })
    .state('base.console.edit.cache-service', {
        url: '/cache-service',
        component: 'pageConsoleCacheService',
        permission: 'management',
        resolve: {
            _shortCachesAndModels: shortCachesAndModels,            
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        tfMetaTags: {
            title: 'Cache Service Console'
        }
    })
    .state('base.console.edit.service.select', {
        url: `/{serviceID}`,
        permission: 'management',
        resolve: {

        },
        data: {
            errorState: 'base.console.edit.service'
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        tfMetaTags: {
            title: 'Invoke Service'
        }
    })
    .state('base.console.edit.cache-service.select', {
        url: `/{cacheID}`,
        permission: 'management',
        resolve: {
            _cache: ['ConfigEffects', '$transition$', ({etp}, $transition$) => {
                const {clusterID, cacheID} = $transition$.params();

                if (cacheID === 'new' || !cacheID)
                    return Promise.resolve();

                return etp('LOAD_CACHE', {cacheID});
            }]
        },
        data: {
            errorState: 'base.console.edit.cache-service'
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        tfMetaTags: {
            title: 'Invoke Cache Service'
        }
    })
    .state('base.console.edit.advanced', {
        url: '/advanced',
        component: 'pageConsoleAdvanced',
        permission: 'management',
        redirectTo: 'base.console.edit.advanced.cluster'
    })
    .state('base.console.edit.advanced.cluster', {
        url: '/cluster',
        component: pageConsoleAdvancedClusterComponent.name,
        permission: 'management',
        resolve: {
            _shortCaches: shortCachesResolve
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        tfMetaTags: {
            title: 'Control Cluster'
        }
    })
    .state('base.console.edit.advanced.cluster.command', {
        url: `/{serviceID}`,
        permission: 'management',
        resolve: {
            
        },
        data: {
            errorState: 'base.console.edit.advanced.cluster'
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        tfMetaTags: {
            title: 'Invoke Cluster Command'
        }
    })
    .state('base.console.edit.advanced.caches', {
        url: '/caches',
        permission: 'management',
        component: pageConsoleAdvancedCachesComponent.name,
        resolve: {            
            _shortCachesAndModels: shortCachesAndModels
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        tfMetaTags: {
            title: 'Configure Caches'
        }
    })
    .state('base.console.edit.advanced.caches.cache', {
        url: `/{cacheID}`,
        permission: 'management',
        resolve: {
            _cache: ['ConfigEffects', '$transition$', ({etp}, $transition$) => {
                const {clusterID, cacheID} = $transition$.params();

                if (cacheID === 'new')
                    return Promise.resolve();

                return etp('LOAD_CACHE', {cacheID});
            }]
        },
        data: {
            errorState: 'base.console.edit.advanced.caches'
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        tfMetaTags: {
            title: 'Configure Caches'
        }
    })
    .state('base.console.edit.advanced.models', {
        url: '/models',
        component: pageConsoleAdvancedModelsComponent.name,
        permission: 'management',
        resolve: {
            _shortCachesAndModels: shortCachesAndModels
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        tfMetaTags: {
            title: 'Configure SQL Schemes'
        }
    })
    .state('base.console.edit.advanced.models.model', {
        url: `/{modelID}`,
        resolve: {
            _cache: ['ConfigEffects', '$transition$', ({etp}, $transition$) => {
                const {clusterID, modelID} = $transition$.params();

                if (modelID === 'new')
                    return Promise.resolve();

                return etp('LOAD_MODEL', {modelID});
            }]
        },
        data: {
            errorState: 'base.console.edit.advanced.models'
        },
        permission: 'management',
        resolvePolicy: {
            async: 'NOWAIT'
        }
    });
}

registerStates.$inject = ['$stateProvider'];

export {registerStates};
