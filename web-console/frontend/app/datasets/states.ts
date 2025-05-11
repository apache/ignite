import {StateParams} from '@uirouter/angularjs';
import {from, combineLatest} from 'rxjs';
import {switchMap, take, map} from 'rxjs/operators';

function registerStates($stateProvider) {
    // Setup the states.
    $stateProvider
    .state('base.datasets', {
        permission: 'query',
        url: '/datasets',
        onEnter: ['ConfigureState', (ConfigureState) => ConfigureState.dispatchAction({type: 'PRELOAD_STATE', state: {}})],
        resolve: {           
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        redirectTo: 'base.datasets.overview'
    })
    .state('base.datasets.overview', {
        url: '/overview',
        component: 'pageDatasetsOverview',
        permission: 'query',
        tfMetaTags: {
            title: 'Datasets List'
        }
    })
    .state('base.datasets.edit', {
        url: `/{datasetID}`,
        permission: 'query',
        component: 'pageDatasets',
        resolve: {
            _dataset: ['$transition$', ($transition$) => {
                return {datasetID: $transition$.params().datasetID};
            }]
        },
        data: {
            errorState: 'base.datasets.overview'
        },
        redirectTo: 'base.datasets.edit.basic',
        failState: 'signin',
        tfMetaTags: {
            title: 'Dataset'
        }
    })
    .state('base.datasets.edit.basic', {
        url: '/basic',
        component: 'pageDatasetsBasic',
        permission: 'query',
        resolve: {            
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        tfMetaTags: {
            title: 'Basic Configuration'
        }
    })
    .state('base.datasets.edit.advanced', {
        url: '/advanced',
        component: 'pageDatasetsAdvanced',
        permission: 'query',
        resolve: {            
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        tfMetaTags: {
            title: 'Advanced Configuration'
        }
    })
    .state('base.datasets.edit.china-map', {
        url: '/china-map',
        component: 'pageChinaMap',
        permission: 'query',
        resolve: {            
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        tfMetaTags: {
            title: 'Data distribution'
        }
    })
}

registerStates.$inject = ['$stateProvider'];

export {registerStates};
