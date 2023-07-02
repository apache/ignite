/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {StateParams} from '@uirouter/angularjs';

import {from, combineLatest} from 'rxjs';
import {switchMap, take, map} from 'rxjs/operators';

export type ClusterParams = ({clusterID: string} | {clusterID: 'new'}) & StateParams;


function registerStates($stateProvider) {
    // Setup the states.
    $stateProvider
    .state('base.datasource', {
        permission: 'configuration',
        url: '/datasource',
        onEnter: ['ConfigureState', (ConfigureState) => ConfigureState.dispatchAction({type: 'PRELOAD_STATE', state: {}})],
        resolve: {
           
        },
        resolvePolicy: {
            async: 'NOWAIT'
        },
        redirectTo: 'base.datasource.overview'
    })
    .state('base.datasource.overview', {
        url: '/overview',
        component: 'pageDatasourceOverview',
        permission: 'configuration',
        tfMetaTags: {
            title: 'DataSource List'
        }
    })
    .state('base.datasource.edit', {
        url: `/{clusterID}`,
        permission: 'configuration',
        component: 'pageDatasource',
        resolve: {
           
        },
        data: {
            errorState: 'base.datasource.overview'
        },
        redirectTo: 'base.datasource.edit.basic',
        failState: 'signin',
        tfMetaTags: {
            title: 'Datasource'
        }
    })
    .state('base.datasource.edit.basic', {
        url: '/basic',
        component: 'pageDatasourceBasic',
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
    .state('base.datasource.edit.advanced', {
        url: '/advanced',
        component: 'pageDatasourceAdvanced',
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
    
    ;
}

registerStates.$inject = ['$stateProvider'];

export {registerStates};
