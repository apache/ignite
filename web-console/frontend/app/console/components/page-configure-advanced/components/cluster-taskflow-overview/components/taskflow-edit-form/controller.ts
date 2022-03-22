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

import {Subject, Observable, combineLatest, from, of} from 'rxjs';
import {tap,take,switchMap,catchError } from 'rxjs/operators';
import {Menu} from 'app/types';

import LegacyConfirmFactory from 'app/services/Confirm.service';
import Version from 'app/services/Version.service';
import Caches from 'app/configuration/services/Caches';
import FormUtilsFactory from 'app/services/FormUtils.service';
import AgentManager from 'app/modules/agent/AgentManager.service';
import TaskFlows from 'app/console/services/TaskFlows';

export default class TaskFlowFormController {
    

    static $inject = ['IgniteConfirm', 'IgniteVersion', '$scope', 'Caches', 'TaskFlows', 'IgniteFormUtils', 'AgentManager'];

    constructor(
        private IgniteConfirm: ReturnType<typeof LegacyConfirmFactory>,
        private IgniteVersion: Version,
        private $scope: ng.IScope,
        private Caches: Caches,
        private TaskFlows: TaskFlows,
        private IgniteFormUtils: ReturnType<typeof FormUtilsFactory>,
        private AgentManager: AgentManager
    ) {
       this.taskFlow = this.TaskFlows.getBlankTaskFlow();
    }

    onSave: ng.ICompiledExpression;
    
    sourceCluster: object;
    
    sourceCaches: Array<string>;
    
    targetCluster: Array<string>;
    
    $onInit() {
        
        this.$scope.ui = this.IgniteFormUtils.formUI();

        this.formActions = [
            {text: 'Save TaskFlow', icon: 'checkmark', click: () => this.confirmAndSave()}                      
        ];
    }

    $onDestroy() {
        
    }
    
    buildTaskFlows(tplFlow){
      
       this.taskFlow.sourceCluster = this.sourceCluster.id;  
       this.taskFlow.group = this.sourceCluster.name;
       this.taskFlow.targetCluster = this.targetCluster[0];
       let taskList = []
       for(let cache of this.sourceCaches){
           this.taskFlow = Object.assign(this.taskFlow,tplFlow);
           this.taskFlow.sourceCache = cache.name;
           this.taskFlow.name = 'Data from '+cache.name+' to '+this.targetCluster[0];
           taskList.push(this.taskFlow);
       }
       return taskList;
    }

    $onChanges(changes) {
        if (
            'taskFlow' in changes && get(this.clonedTaskFlow, 'id') !== get(this.taskFlow, 'id')
        ) {
            this.clonedTaskFlow = cloneDeep(changes.taskFlow.currentValue);
            if (this.$scope.ui && this.$scope.ui.inputForm) {
                this.$scope.ui.inputForm.$setPristine();
                this.$scope.ui.inputForm.$setUntouched();
            }
        }
        
    }

    getValuesToCompare() {
        return [this.taskFlow, this.clonedTaskFlow];
    }    
    
    saveTaskFlowForGrid(taskId:string,tplFlow) {
        let args = this.onSave({$event:{id: taskId,args: tplFlow}});
        
        let tasks = this.buildTaskFlows(tplFlow);
        let result = {};
        for(let task of tasks){           
            result[task.id] = from(this.TaskFlows.saveBasic(task)).pipe(
                switchMap(({data}) => of(                   
                    {type: 'EDIT_TASK_FLOW', taskFlow: data},
                    {type: 'SAVE_AND_EDIT_TASK_FLOW_OK'}
                )),
                catchError((error) => of({
                    type: 'SAVE_AND_EDIT_TASK_FLOW_ERR',
                    error: {
                        message: `Failed to save cluster task flow: ${error.data.message}.`
                    }
                }))
            );            
        }
        if(!result){
            this.AgentManager.callClusterCommand({id: clusterId},serviceName,params).then((data) => {
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
        
    }

    reset = (forReal) => forReal ? this.clonedTaskFlow = cloneDeep(this.taskFlow) : void 0;

    confirmAndSave() {
        if (this.$scope.ui.inputForm && this.$scope.ui.inputForm.$invalid)
            return this.IgniteFormUtils.triggerValidation(this.$scope.ui.inputForm, this.$scope);
        return this.IgniteConfirm.confirm('Are you sure you want to save task flow ' + this.taskFlow.name + ' for current grid?')
        .then(() => { this.saveTaskFlowForGrid(this.taskFlow.id, this.clonedTaskFlow); } );
    }

    clearImplementationVersion(storeFactory) {
        delete storeFactory.implementationVersion;
    }
}
