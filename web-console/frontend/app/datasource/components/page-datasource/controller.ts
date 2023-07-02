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

import get from 'lodash/get';
import {Observable, combineLatest, from, of} from 'rxjs';
import {map, tap, pluck, take, filter, catchError, distinctUntilChanged, switchMap, publishReplay, refCount} from 'rxjs/operators';
import ConfigureState from 'app/configuration/services/ConfigureState';
import Datasource from 'app/datasource/services/Datasource';
import {UIRouter} from '@uirouter/angularjs';

export default class PageDatasourceController {
    static $inject = ['$uiRouter', 'ConfigureState', 'Datasource'];

    constructor(
        private $uiRouter: UIRouter,
        private ConfigureState: ConfigureState,
        private Datasource: Datasource
        
    ) {}

    clusterID$: Observable<string>;
    clusterName$: Observable<string>;
    tooltipsVisible = true;

    $onInit() {
        this.clusterID$ = this.$uiRouter.globals.params$.pipe(pluck('clusterID'));

        
        let cluster$ = this.clusterID$.pipe(
            distinctUntilChanged(),
            switchMap((id) => {
                return from(this.Datasource.selectDatasource(id));
            }),
            distinctUntilChanged(),
            publishReplay(1),
            refCount()
        );  
               

        const isNew$ = this.clusterID$.pipe(map((v) => v === 'new'));

        this.clusterName$ = combineLatest(cluster$, isNew$, (cluster, isNew) => {
            return `${isNew ? 'Create' : 'Edit'} DataSource configuration 
            ${isNew ? '' : `‘${get(cluster, 'jndiName','')}’`}`;
        });
    }

    $onDestroy() {}
}
