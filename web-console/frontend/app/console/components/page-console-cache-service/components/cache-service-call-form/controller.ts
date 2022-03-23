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

import cloneDeep from 'lodash/cloneDeep';
import get from 'lodash/get';
import {tap} from 'rxjs/operators';
import {Menu} from 'app/types';

import LegacyConfirmFactory from 'app/services/Confirm.service';
import Version from 'app/services/Version.service';
import Caches from 'app/configuration/services/Caches';
import FormUtilsFactory from 'app/services/FormUtils.service';
import AgentManager from 'app/modules/agent/AgentManager.service';

export default class CacheServiceCallFormController {
    modelsMenu: Menu<string>;

    onCall: ng.ICompiledExpression;
    
    clusterId: string;

    static $inject = ['IgniteConfirm', 'IgniteVersion', '$scope', 'Caches', 'IgniteFormUtils', 'AgentManager'];

    constructor(
        private IgniteConfirm: ReturnType<typeof LegacyConfirmFactory>,
        private IgniteVersion: Version,
        private $scope: ng.IScope,
        private Caches: Caches,
        private IgniteFormUtils: ReturnType<typeof FormUtilsFactory>,
        private AgentManager: AgentManager
    ) {}

    $onInit() {
        this.available = this.IgniteVersion.available.bind(this.IgniteVersion);

        const rebuildDropdowns = () => {
            this.$scope.affinityFunction = [
                {value: 'Rendezvous', label: 'Rendezvous'},
                {value: 'Custom', label: 'Custom'},
                {value: null, label: 'Default'}
            ];

            if (!this.IgniteVersion.currentSbj.getValue().hiveVersion
                && _.get(this.clonedCache, 'cacheStoreFactory.kind') === 'HiveCacheJdbcPojoStoreFactory')
                this.clonedCache.cacheStoreFactory.kind = null;
        };

        rebuildDropdowns();

        const filterModel = () => {
            if (
                this.clonedCache &&
                this.available('2.0.0') &&
                get(this.clonedCache, 'affinity.kind') === 'Fair'
            )
                this.clonedCache.affinity.kind = null;

        };

        this.subscription = this.IgniteVersion.currentSbj.pipe(
            tap(rebuildDropdowns),
            tap(filterModel)
        )
        .subscribe();

        // TODO: Do we really need this?
        this.$scope.ui = this.IgniteFormUtils.formUI();

        this.formActions = [
            {text: 'Load Data', icon: 'checkmark', click: () => this.confirmAndLoad(false)},
            {text: 'Load Updated Data', icon: 'download', click: () => this.confirmAndLoad(true)},
            {text: 'Clear Data', icon: 'checkmark', click: () => this.confirmAndClear()},
            {text: 'Poll Remote Data', icon: 'checkmark', click: () => this.confirmAndCopy(true)}            
        ];
    }

    $onDestroy() {
        this.subscription.unsubscribe();
    }

    $onChanges(changes) {
        if (
            'cache' in changes && get(this.clonedCache, 'id') !== get(this.cache, 'id')
        ) {
            this.clonedCache = cloneDeep(changes.cache.currentValue);
            if (this.$scope.ui && this.$scope.ui.inputForm) {
                this.$scope.ui.inputForm.$setPristine();
                this.$scope.ui.inputForm.$setUntouched();
            }
        }
        if ('models' in changes)
            this.modelsMenu = (changes.models.currentValue || []).map((m) => ({value: m.id, label: m.valueType}));
    }

    getValuesToCompare() {
        return [this.cache, this.clonedCache].map(this.Caches.normalize);
    }

    loadData(updated:boolean) {        
        let serviceName = 'loadDataService';
        return this.callServiceForCache(serviceName,{updated});
    }
    
    copyData(updated:boolean) {        
        let serviceName = 'copyDataService';
        return this.callServiceForCache(serviceName,{updated});
    }
    
    clearData(){
        let serviceName = 'clearDataService';
        return this.callServiceForCache(serviceName,{});
    }
    
    callServiceForCache(serviceName:string,params) {
        let args = this.onCall({$event: {cache: this.clonedCache}});
        let clusterId = args['id'];
        params = Object.assign(args,params);
        this.AgentManager.callClusterService({id: clusterId},serviceName,params).then((data) => {  
            this.$scope.status = data.status; 
            if(data.result){
                return data.result;
            }    
            else if(data.message){
                this.$scope.message = data.message;
            }  
            return {}
        })   
       .catch((e) => {
            this.$scope.message = ('Failed to callClusterService : '+serviceName+' Caused : '+e);           
        });
    }

    reset = (forReal) => forReal ? this.clonedCache = cloneDeep(this.cache) : void 0;

    confirmAndClear() {
        return this.IgniteConfirm.confirm('Are you sure you want to clear all data for current cache?')
        .then(() => { this.clearData(); } );
    }
    
    confirmAndLoad(updated:boolean) {        
        if (this.$scope.ui.inputForm && this.$scope.ui.inputForm.$invalid)
            return this.IgniteFormUtils.triggerValidation(this.$scope.ui.inputForm, this.$scope);
        return this.IgniteConfirm.confirm('Are you sure you want to load all data for current cache?')
        .then( () => { this.loadData(updated); } );
    }
    
    confirmAndCopy(updated:boolean) {        
        if (this.$scope.ui.inputForm && this.$scope.ui.inputForm.$invalid)
            return this.IgniteFormUtils.triggerValidation(this.$scope.ui.inputForm, this.$scope);
        return this.IgniteConfirm.confirm('Are you sure you want to poll data from remote cluster to this  cache?')
        .then( () => { this.copyData(updated); } );
    }

    clearImplementationVersion(storeFactory) {
        delete storeFactory.implementationVersion;
    }
}
