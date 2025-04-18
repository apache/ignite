

import {StateParams} from '@uirouter/angularjs';
import pageConfigureAdvancedClusterComponent from './components/page-configure-advanced/components/page-configure-advanced-cluster/component';
import pageConfigureAdvancedServicesComponent from './components/page-configure-advanced/components/page-configure-advanced-services/component';
import pageConfigureAdvancedModelsComponent from './components/page-configure-advanced/components/page-configure-advanced-models/component';
import pageConfigureAdvancedCachesComponent from './components/page-configure-advanced/components/page-configure-advanced-caches/component';

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
    .state('base.configuration', {
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
        },
        redirectTo: 'base.configuration.overview'
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
        url: `/{clusterID}`,
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
            return from(Promise.all(waitFor)).pipe(
                switchMap(() => {
                    return combineLatest(
                        ConfigureState.state$.pipe(ConfigSelectors.selectCluster($transition$.params().clusterID), take(1)),
                        ConfigureState.state$.pipe(ConfigSelectors.selectShortClusters(), take(1))
                    );
                }),
                map(([cluster = {caches: []}, clusters]) => {
                    return (clusters.value.size > 10 || cluster.caches.length > 5)
                        ? 'base.configuration.edit.advanced'
                        : 'base.configuration.edit.basic';
                })
            )
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
    .state('base.configuration.edit.advanced.services', {
        url: '/services',
        component: pageConfigureAdvancedServicesComponent.name,
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
            _shortCachesAndModels: shortCachesAndModels
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        tfMetaTags: {
            title: 'Configure Caches'
        }
    })
    .state('base.configuration.edit.advanced.caches.cache', {
        url: `/{cacheID}`,
        permission: 'configuration',
        resolve: {
            _cache: ['ConfigEffects', '$transition$', ({etp}, $transition$) => {
                const {clusterID, cacheID} = $transition$.params();

                if (cacheID === 'new')
                    return Promise.resolve();

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
            _shortCachesAndModels: shortCachesAndModels
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        tfMetaTags: {
            title: 'Configure SQL Schemes'
        }
    })
    .state('base.configuration.edit.advanced.models.model', {
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
            errorState: 'base.configuration.edit.advanced.models'
        },
        permission: 'configuration',
        resolvePolicy: {
            async: 'NOWAIT'
        }
    });
}

registerStates.$inject = ['$stateProvider'];

export {registerStates};
