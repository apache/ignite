

import get from 'lodash/get';
import omit from 'lodash/fp/omit';
import uuidv4 from 'uuid/v4';
import {of, empty, combineLatest, forkJoin, pipe, from} from 'rxjs';
import {filter, pluck, map, switchMap, take, distinctUntilChanged, exhaustMap} from 'rxjs/operators';
import {uniqueName} from 'app/utils/uniqueName';
import {DatasourceDto} from 'app/configuration/types';
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
    
    getBlankDatasource():DatasourceDto {
        return {
            jndiName: '',
            schemaName: '',
            jdbcUrl:  'jdbc:postgresql://[host]:[port]/[database]',
            driverCls: 'org.postgresql.Driver',
            db: 'Generic',
            jdbcProp: {},
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
            const datasource = result.data 
            if(datasource.jdbcProp==null){
                datasource.jdbcProp = {}
            }
            datasource.attributes = []
            for(let [name,value] of Object.entries(datasource.jdbcProp)){
                if(name=='rebalanceBatchSize'){                   
                    datasource.rebalanceBatchSize = value
                }
                else{
                    datasource.attributes.push({name:name,value:value})
                }
            }
            
            return datasource;
        });
    }
    
    getDatasourceList(){
        return this.$http.get(`/api/v1/datasource`);
    }
    
    removeDatasource(id: string) {
        return this.$http.delete(`/api/v1/datasource/${id}`);
    }
    
    saveBasic(datasource:DatasourceDto) {
        return this.$http.put('/api/v1/datasource', datasource);
    }
    
    saveAdvanced(datasource:DatasourceDto) {        
        return this.$http.put('/api/v1/datasource', datasource);
    }
}
