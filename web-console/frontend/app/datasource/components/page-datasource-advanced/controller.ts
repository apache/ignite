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

import {Subject, Observable, from, of} from 'rxjs';
import {take, pluck, switchMap, map, filter, catchError, distinctUntilChanged, publishReplay, refCount} from 'rxjs/operators';
import {UIRouter} from '@uirouter/angularjs';


import ConfigureState from 'app/configuration/services/ConfigureState';
import Datasource from 'app/datasource/services/Datasource';

// Controller for Datasource screen.
export default class PageConfigureAdvancedDatasource {
    static $inject = ['$uiRouter', 'Datasource', 'ConfigureState'];

    constructor(
        private $uiRouter: UIRouter,
        private Datasource: Datasource,
        private ConfigureState: ConfigureState
    ) {}

    $onInit() {
        const clusterID$ = this.$uiRouter.globals.params$.pipe(
            take(1),
            pluck('clusterID'),
            filter((v) => v),
            take(1)
        );
        
        this.originalCluster$ = clusterID$.pipe(
            distinctUntilChanged(),
            switchMap((id) => {
                return from(this.Datasource.selectDatasource(id));
            }),
            distinctUntilChanged(),
            publishReplay(1),
            refCount()
        );

        this.isNew$ = this.$uiRouter.globals.params$.pipe(pluck('clusterID'), map((id) => id === 'new'));

        this.isBlocked$ = clusterID$;
    }

    save({datasource,redirect}) {        
        let stat = from(this.Datasource.saveAdvanced(datasource)).pipe(
            switchMap(({data}) => of(                   
                {type: 'EDIT_TASK_FLOW', datasource: data},
                {type: 'SAVE_AND_EDIT_TASK_FLOW_OK'}
            )),
            catchError((error) => of({
                type: 'SAVE_AND_EDIT_DATASOURCE_ERR',
                error: {
                    message: `Failed to save datasource : ${error.data.message}.`
                }
            }))
        );
        stat.subscribe(
            next => {                
                if(redirect){
                    return this.$uiRouter.stateService.go('/datasource');
                }
            }
        );        
        
    }
}
