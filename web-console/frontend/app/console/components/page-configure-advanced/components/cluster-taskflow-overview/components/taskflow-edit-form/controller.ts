

import cloneDeep from 'lodash/cloneDeep';
import get from 'lodash/get';

import {Subject, Observable, combineLatest, from, of} from 'rxjs';
import {catchError,tap, map, refCount, pluck, take, filter, publishReplay, switchMap, distinctUntilChanged} from 'rxjs/operators';
import {Menu} from 'app/types';

import {default as ConfigureState} from 'app/configuration/services/ConfigureState';
import {default as ConfigSelectors} from 'app/configuration/store/selectors';
import LegacyConfirmFactory from 'app/services/Confirm.service';
import Version from 'app/services/Version.service';
import Caches from 'app/configuration/services/Caches';
import Clusters from 'app/configuration/services/Clusters';
import FormUtilsFactory from 'app/services/FormUtils.service';
import AgentManager from 'app/modules/agent/AgentManager.service';
import TaskFlows from 'app/console/services/TaskFlows';

export default class TaskFlowFormController {
    

    static $inject = ['IgniteConfirm', 'IgniteVersion', '$scope','ConfigureState','ConfigSelectors','Caches','Clusters', 'TaskFlows', 'IgniteFormUtils', 'AgentManager'];

    constructor(
        private IgniteConfirm: ReturnType<typeof LegacyConfirmFactory>,
        private IgniteVersion: Version,
        private $scope: ng.IScope,
        private ConfigureState: ConfigureState,
        private ConfigSelectors: ConfigSelectors,
        private Caches: Caches,
        private Clusters: Clusters,
        private TaskFlows: TaskFlows,
        private IgniteFormUtils: ReturnType<typeof FormUtilsFactory>,
        private AgentManager: AgentManager
    ) {
       this.taskFlow = this.TaskFlows.getBlankTaskFlow();
    }

    onSave: ng.ICompiledExpression;
    
    sourceCluster: object;
    
    targetCaches: Array<object>;
    
    targetModels: Array<object>;
    
    targetCluster: object;
    
    targetClusterId: string;
    
    $onInit() {
        
        this.originalCluster$ = this.sourceCluster.pipe(
            filter((v) => v.length==1),
            distinctUntilChanged(),
            switchMap((items) => {
                this.sourceClusterId = items[0].id;    
                return from(this.Clusters.getConfiguration(this.sourceClusterId));               
            }),            
            publishReplay(1),
            refCount()
        );   
             
        this.subscrition = this.originalCluster$.subscribe((c) =>{
            if(c && c.data.cluster){
                let cluster = c.data.cluster;
                this.taskFlow.name = 'receive data from '+ cluster.name;
                this.taskFlow.group = this.targetClusterId;
                this.taskFlow.sourceCluster = cluster.id;
                this.clonedTaskFlow = cloneDeep(this.taskFlow);
            }            
        });
        
        this.$scope.ui = this.IgniteFormUtils.formUI();

        this.formActions = [
            {text: 'Save TaskFlow', icon: 'checkmark', click: () => this.confirmAndSave()},
            {text: 'Start TaskFlow', icon: 'checkmark', click: () => this.confirmAndStart()}                
        ];
    }

    $onDestroy() {
        this.subscrition.unsubscribe();
    }
    
    buildTaskFlows(tplFlow){      
       tplFlow.group = this.targetClusterId;
       tplFlow.targetCluster = this.targetClusterId;
       let taskList = []
       for(let cache of this.targetCaches){
           this.taskFlow = Object.assign({},tplFlow);
           this.taskFlow.target = cache.name;
           this.taskFlow.source = cache.name;
           this.taskFlow.name = 'Data from '+this.taskFlow.source+' to '+cache.name;
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
    
    saveTaskFlowForGrid(tplFlow) {
        let args = this.onSave({$event:{sourceCluster: tplFlow.sourceCluster,args: tplFlow}});
        
        let tasks = this.buildTaskFlows(tplFlow);
        let result = [];
        
        for(let task of tasks){           
            let stat = from(this.TaskFlows.saveBasic(task)).pipe(
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
            result.push(stat);
            
            stat.subscribe((d)=>{
                if(d.error){
                    this.$scope.message = d.error.message;
                }
            });
        }
        if(!result){
            this.$scope.message = 'no task save!';
        }        
    }
    
    startTaskFlowForGrid(tplFlow) {
        let tasks = this.buildTaskFlows(tplFlow);
        let result = [];
        let serviceName = 'computeTaskLoadService';
        let task = 'ContinuousMapperTask';
        for(let task of tasks){           
            let stat = from(this.TaskFlows.getTaskFlows(task.group,task.target,task.source)).pipe(
                switchMap(({data}) => of(                   
                    {type: 'LOAD_TASK_FLOW', taskFlow: data}                   
                )),
                catchError((error) => of({
                    type: 'LOAD_TASK_FLOW_ERR',
                    error: {
                        message: `Failed to save cluster task flow: ${error.data.message}.`
                    }
                }))
            );    
            result.push(stat);
        }
        if(result){
            this.AgentManager.callClusterService({id: tplFlow.sourceCluster},serviceName,{tasks,task,targetModel:this.targetModels}).then((data) => {
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
        .then(() => { this.saveTaskFlowForGrid(this.clonedTaskFlow); } );
    }
    
    confirmAndStart() {        
        return this.IgniteConfirm.confirm('Are you sure you want to start this task flow ' + this.taskFlow.name + ' for current grid?')
        .then(() => { this.startTaskFlowForGrid(this.clonedTaskFlow); } );
    }

    clearImplementationVersion(storeFactory) {
        delete storeFactory.implementationVersion;
    }
}
