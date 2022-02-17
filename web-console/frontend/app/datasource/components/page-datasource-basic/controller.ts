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

import {forkJoin, merge} from 'rxjs';
import {map, tap, pluck, take, filter, distinctUntilChanged, switchMap, publishReplay, refCount} from 'rxjs/operators';
import cloneDeep from 'lodash/cloneDeep';
import get from 'lodash/get';
import naturalCompare from 'natural-compare-lite';



import {Confirm} from 'app/services/Confirm.service';

import ConfigureState from '../../../configuration/services/ConfigureState';
import DatasourceSelectors from '../../store/selectors';

import {UIRouter} from '@uirouter/angularjs';
import FormUtils from 'app/services/FormUtils.service';
import AgentManager from 'app/modules/agent/AgentManager.service';


export default class PageDatasourceBasicController {
    form: ng.IFormController;

    static $inject = [
        'Confirm', '$uiRouter', 'ConfigureState', 'DatasourceSelectors',  '$element', 'IgniteFormUtils', 'AgentManager', '$scope'
    ];

    constructor(
        private Confirm: Confirm,
        private $uiRouter: UIRouter,
        private ConfigureState: ConfigureState,
        private DatasourceSelectors: DatasourceSelectors,
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

        const clusterID$ = this.$uiRouter.globals.params$.pipe(
            take(1),
            pluck('clusterID'),
            filter((v) => v),
            take(1)
        );
        this.clusterID$ = clusterID$;

        this.isNew$ = this.$uiRouter.globals.params$.pipe(pluck('clusterID'), map((id) => id === 'new'));
        
        this.shortClusters$ = this.ConfigureState.state$.pipe(this.DatasourceSelectors.selectShortClustersValue());
        this.originalCluster$ = clusterID$.pipe(
            distinctUntilChanged(),
            switchMap((id) => {
                return this.ConfigureState.state$.pipe(this.DatasourceSelectors.selectClusterToEdit(id));
            }),
            distinctUntilChanged(),
            publishReplay(1),
            refCount()
        );  
       
       this.formActionsMenu = [
           {
               text: 'Save',
               click: () => this.save(),
               icon: 'checkmark'
           },
           {
               text: 'Save and Public',
               click: () => this.save(true),
               icon: 'download'
           }
       ];
       
    }

   

    save(download = false) {
        if (this.form.$invalid)
            return this.IgniteFormUtils.triggerValidation(this.form, this.$scope);

        this.ConfigureState.dispatchAction((download ? basicSaveAndDownload : basicSave)(cloneDeep(this.clonedCluster)));
    }

    reset() {
        this.clonedCluster = cloneDeep(this.originalCluster);
        this.ConfigureState.dispatchAction({type: 'RESET_EDIT_CHANGES'});
    }

    restart() {
        this.AgentManager.startCluster(this.clonedCluster).then((msg) => {  
            if(!msg.message){
               this.$scope.status = msg.status;
               this.ConfigureState.dispatchAction({type: 'RESTART_CLUSTER'});
               this.clonedCluster.status = msg.status;
            }            
            this.$scope.message = msg.message;
 
        })
       .catch((e) => {
            this.$scope.message = ('Failed to generate project config file: '+e);           
        });
    }

    stop() {
        this.AgentManager.stopCluster(this.clonedCluster).then((msg) => {  
    	    if(!msg.message){
               this.$scope.status = msg.status;
               this.ConfigureState.dispatchAction({type: 'RESTART_CLUSTER'});
               this.clonedCluster.status = msg.status;
            }            
            this.$scope.message = msg.message;
        });        
    }
    
    callService(serviceName,args) {
        this.AgentManager.callClusterService(this.clonedCluster,serviceName,args).then((data) => {  
            this.$scope.status = data.status;               
            this.clonedCluster.status = data.status;
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

    isStoped() {
        return this.$scope.status !== 'started';
    }

    confirmAndReset() {
        return this.Confirm.confirm('Are you sure you want to undo all changes for current cluster?')
            .then(() => this.reset())
            .catch(() => {});
    }
    confirmAndRestart() {
        return this.Confirm.confirm('Are you sure you want to restart current cluster? Current status:' + this.clonedCluster.status)
            .then(() => this.restart())
            .catch(() => {});
    }
    confirmAndStop() {
        return this.Confirm.confirm('Are you sure you want to stop current cluster?  Current status:' + this.clonedCluster.status)
            .then(() => this.stop())
            .catch(() => {});
    }
}
