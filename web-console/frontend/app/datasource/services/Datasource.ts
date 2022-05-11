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
import omit from 'lodash/fp/omit';
import uuidv4 from 'uuid/v4';
import {of, empty, combineLatest, forkJoin, pipe, from} from 'rxjs';
import {filter, pluck, map, switchMap, take, distinctUntilChanged, exhaustMap} from 'rxjs/operators';
import {uniqueName} from 'app/utils/uniqueName';
import {DatasourceDto} from '../types';
import {Menu} from 'app/types';

const selectValues = map((v) => v && [...v.value.values()]);

const selectItemToEdit = ({items, itemFactory, defaultName = '', itemID}) => switchMap((item) => {
    if (item)
        return of(Object.assign(itemFactory(), item));

    if (itemID === 'new')
        return items.pipe(take(1), map((items) => Object.assign(itemFactory(), {name: uniqueName(defaultName, items)})));

    if (!itemID)
        return of(null);

    return empty();
});

export default class Datasource {
    static $inject = ['$http'];

    constructor(private $http: ng.IHttpService) {}
    
    getBlankDatasource() {
        return {
            jndiName: '',
            jdbcUrl:  'jdbc:dbtype://[host]:[port]/[database]',
            id: uuidv4()
        }
     }
    
    /**
     * @returns {(state$: Observable) => Observable<Array<ShortCluster>>}
     */
    selectDatasourceValue = () => (state$) => state$.pipe(selectValues);
    
    selectDatasource = (id) => {
        if(id=='new'){
            return of(this.getBlankDatasource())
        }
        return this.getDatasource(id);
    }
    
    selectDatasourceToEdit = (clusterID, defaultName = 'DataSource') => (state$) => state$.pipe(
        this.selectDatasource(clusterID),
        distinctUntilChanged(),
        selectItemToEdit({
            items: state$.pipe(this.selectDatasourceValue()),
            itemFactory: () => this.getBlankDatasource(),
            defaultName,
            itemID: clusterID
        })
    );

    getDatasource(id: string){
        return this.$http.get(`/api/v1/datasource/${id}`).then((result) => {
            return result.data;
        });
    }
    
    getDatasourceList(){
        return this.$http.get(`/api/v1/datasource`);
    }
    
    removeDatasource(id: string) {
        return this.$http.delete(`/api/v1/datasource/${id}`);
    }
    
    saveBasic(datasource) {
        return this.$http.put('/api/v1/datasource', datasource);
    }
    
    saveAdvanced(datasource) {
        return this.$http.put('/api/v1/datasource', datasource);
    }
}
