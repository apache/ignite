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

import {forkJoin, merge, from, of} from 'rxjs';
import {map, tap, pluck, take, filter, catchError, distinctUntilChanged, switchMap, publishReplay, refCount} from 'rxjs/operators';
import cloneDeep from 'lodash/cloneDeep';
import get from 'lodash/get';
import naturalCompare from 'natural-compare-lite';



import {Confirm} from 'app/services/Confirm.service';

import ConfigureState from 'app/configuration/services/ConfigureState';
import Datasource from 'app/datasource/services/Datasource';

import {UIRouter} from '@uirouter/angularjs';
import FormUtils from 'app/services/FormUtils.service';
import AgentManager from 'app/modules/agent/AgentManager.service';
import {dbPresets} from 'app/datasource/dbPresets';

export default class PageDatasourceBasicController {
    form: ng.IFormController;

    static $inject = [
        'Confirm', '$uiRouter', 'ConfigureState', 'Datasource',  '$element', 'IgniteFormUtils', 'AgentManager', '$scope'
    ];

    constructor(
        private Confirm: Confirm,
        private $uiRouter: UIRouter,
        private ConfigureState: ConfigureState,
        private Datasource: Datasource,
        private $element: JQLite,        
        private IgniteFormUtils: ReturnType<typeof FormUtils>,
        private AgentManager: AgentManager,      
        private $scope: ng.IScope
    ) {}

    
    
    $onDestroy() {        
        if (this.onBeforeTransition) this.onBeforeTransition();
        this.$element = null;
    }

    $postLink() {
        this.$element.addClass('panel--ignite');
    }

    $onInit() {
        //this.onBeforeTransition = this.$uiRouter.transitionService.onBefore({}, (t) => this._uiCanExit(t));
        this.available = (v) =>{ return true; }
        
        let drivers = [];
        for(let engine of dbPresets){
            let option = {"label": engine.db, "value": engine.jdbcDriverClass}
            drivers.push(option);
        }
        this.drivers = drivers;
        
        const clusterID$ = this.$uiRouter.globals.params$.pipe(
            take(1),
            pluck('clusterID'),
            filter((v) => v),
            take(1)
        );
        this.clusterID$ = clusterID$;

        this.isNew$ = this.$uiRouter.globals.params$.pipe(pluck('clusterID'), map((id) => id === 'new'));
        
        
        this.originalCluster$ = clusterID$.pipe(
            distinctUntilChanged(),
            switchMap((id) => {
                return from(this.Datasource.selectDatasource(id));
            }),
            distinctUntilChanged(),
            publishReplay(1),
            refCount()
        );  
        
        this.originalCluster$.subscribe((c) =>{
            this.clonedCluster = cloneDeep(c);
        })
       
       this.formActionsMenu = [
           {
               text: 'Save',
               click: () => this.save(),
               icon: 'checkmark'
           },
           {
               text: 'Delete',
               click: () => this.confirmAndDelete(),
               icon: 'download'
           }
       ];
       
    }
    
    pingDatasource() {
      let cluster =  this.clonedCluster;
      this.AgentManager.callClusterService(cluster,'datasourceTest',cluster).then((msg) => {
          if(msg.status){
             cluster.status = msg.status;
             this.$scope.status = msg.status;
             this.$scope.message = msg.message;
          }
      });    
    }
    
    disconnectDatasource() {
      let cluster =  this.clonedCluster;
      this.AgentManager.callClusterService(cluster,'datasourceDisconnect').then((msg) => {
          if(msg.status){
             cluster.status = msg.status;
             this.$scope.status = msg.status;
             this.$scope.message = msg.message;
          }
      });    
    }

    save(redirect = false) {
        if (this.form.$invalid)
            return this.IgniteFormUtils.triggerValidation(this.form, this.$scope);
        let datasource = this.clonedCluster
        if(datasource) {
            let stat = from(this.Datasource.saveBasic(datasource)).pipe(
                switchMap(({data}) => of(                   
                    {type: 'EDIT_DATASOURCE', datasource: data},
                    {type: 'SAVE_AND_EDIT_DATASOURCE_OK'}
                )),
                catchError((error) => of({
                    type: 'SAVE_AND_EDIT_DATASOURCE_ERR',
                    error: {
                        message: `Failed to save datasource : ${error.data.message}.`
                    }
                }))
            );            
            this.$scope.message = 'Save successful.'
            if(redirect){
                
                return this.$uiRouter.stateService.go('/datasource');
            } 
        }
        
    }

    reset() {
        this.clonedCluster = cloneDeep(this.originalCluster);
        this.ConfigureState.dispatchAction({type: 'RESET_EDIT_CHANGES'});
    }    

    confirmAndDisconnect() {
        return this.Confirm.confirm('Are you sure you want to disconnect of current datasource?')
            .then(() => this.disconnectDatasource())
            .catch(() => {});
    }
    
    delete(datasource) {
        let stat = from(this.Datasource.removeDatasource(datasource.id)).pipe(
            switchMap(({data}) => of(                   
                {type: 'REMOVE_DATASOURCE', datasource: data},
                {type: 'REMOVE_AND_EDIT_DATASOURCE_OK'}
            )),
            catchError((error) => of({
                type: 'REMOVE_DATASOURCE_ERR',
                error: {
                    message: `Failed to remove datasource : ${error.data.message}.`
                }
            }))
        );    
    }
    
    confirmAndDelete() {
        return this.Confirm.confirm('Are you sure you want to delete current datasource?')
            .then(() => this.delete(this.clonedCluster))
            .catch(() => {});
    }
    
    
    
}
