

import {StateParams} from '@uirouter/angularjs';

import {from, combineLatest} from 'rxjs';
import {switchMap, take, map} from 'rxjs/operators';

function registerStates($stateProvider) {
    // Setup the states.
    $stateProvider
    .state('base.datasets', {
        permission: 'configuration',
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
        permission: 'configuration',
        tfMetaTags: {
            title: 'Datasets List'
        }
    })
    .state('base.datasets.edit', {
        url: `/{datasetID}`,
        permission: 'configuration',
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
        component: 'pageChinaMap',
        permission: 'configuration',
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
        permission: 'configuration',
        resolve: {            
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        tfMetaTags: {
            title: 'Advanced Configuration'
        }
    })
}

registerStates.$inject = ['$stateProvider'];

export {registerStates};
