

import {StateParams} from '@uirouter/angularjs';

import {from, combineLatest} from 'rxjs';
import {switchMap, take, map} from 'rxjs/operators';

function registerStates($stateProvider) {
    // Setup the states.
    $stateProvider
    .state('base.igfs', {
        permission: 'query',
        url: '/igfs',
        onEnter: ['ConfigureState', (ConfigureState) => ConfigureState.dispatchAction({type: 'PRELOAD_STATE', state: {}})],
        resolve: {           
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        redirectTo: 'base.igfs.overview'
    })
    .state('base.igfs.overview', {
        url: '/overview',
        component: 'pageIgfsOverview',
        permission: 'query',
        tfMetaTags: {
            title: 'File Storage List'
        }
    })
    .state('base.igfs.edit', {
        url: `/{storageID}`,
        component: 'pageIgfs',
        permission: 'query',        
        resolve: {
            _storage: ['$transition$', ($transition$) => {
                return {storageID: $transition$.params().storageID};
            }]
        },
        data: {
            errorState: 'base.igfs.overview'
        },
        redirectTo: 'base.igfs.edit.basic',
        failState: 'signin',
        tfMetaTags: {
            title: 'Storage'
        }
    })
    .state('base.igfs.edit.basic', {
        url: '/basic',
        component: 'pageIgfsBasic',
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
    .state('base.igfs.edit.advanced', {
        url: '/advanced',
        component: 'pageIgfsAdvanced',
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
    .state('base.igfs.edit.china-map', {
        url: '/china-map',
        component: 'pageIgfsChinaMap',
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
