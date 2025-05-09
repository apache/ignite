

import cloneDeep from 'lodash/cloneDeep';
import get from 'lodash/get';
import {merge, empty, of, from} from 'rxjs';
import {tap, pluck, publishReplay, catchError, switchMap, distinctUntilChanged, refCount} from 'rxjs/operators';
import {Menu} from 'app/types';
import {UIRouter, TransitionService, StateService} from '@uirouter/angularjs';

import LegacyConfirmFactory from 'app/services/Confirm.service';
import Version from 'app/services/Version.service';
import Caches from 'app/configuration/services/Caches';
import TaskFlows from 'app/console/services/TaskFlows';
import FormUtilsFactory from 'app/services/FormUtils.service';

export default class CacheEditFormController {
    modelsMenu: Menu<string>;

    onSave: ng.ICompiledExpression;

    cache: any;    

    clonedCache: any;

    clusterId: string;

    static $inject = ['$state','IgniteConfirm', 'IgniteVersion', '$scope', 'Caches', 'TaskFlows','IgniteFormUtils'];

    constructor(
        private $state: StateService,
        private IgniteConfirm: ReturnType<typeof LegacyConfirmFactory>,
        private IgniteVersion: Version,       
        private $scope: ng.IScope,
        private Caches: Caches,
        private TaskFlows: TaskFlows,
        private IgniteFormUtils: ReturnType<typeof FormUtilsFactory>
    ) {}
    
    
    $onInit() {
        this.available = this.IgniteVersion.available.bind(this.IgniteVersion);
        
        const rebuildDropdowns = () => {
            this.$scope.affinityFunction = [
                {value: 'Rendezvous', label: 'Rendezvous'},
                {value: 'Custom', label: 'Custom'},
                {value: null, label: 'Default'}
            ];            
        };

        rebuildDropdowns();

        this.subscription = this.IgniteVersion.currentSbj.pipe(
            tap(rebuildDropdowns)            
        )
        .subscribe();        
        
        this.cachesColDefs = [            
            {name: 'Source Cluster:', cellClass: 'pc-form-grid-col-20'},
            {name: 'Source Cache:', cellClass: 'pc-form-grid-col-10'},
            {name: 'Existing Mode:', cellClass: 'pc-form-grid-col-10'},
            {name: 'Atomicity:', cellClass: 'pc-form-grid-col-10', tip: `
                Atomicity:
                <ul>
                    <li>ATOMIC - in this mode distributed transactions and distributed locking are not supported</li>
                    <li>TRANSACTIONAL - in this mode specified fully ACID-compliant transactional cache behavior</li>                    
                </ul>
            `},
            {name: 'Read From Backup:', cellClass: 'pc-form-grid-col-10', tip: `
                Read from source cache used to back up single partition for partitioned cache
            `}
        ]; 
        
        // TODO: Do we really need this?
        this.$scope.ui = this.IgniteFormUtils.formUI();

        this.formActions = [
            {text: 'Save', icon: 'checkmark', click: () => this.save(false)},
            {text: 'Save and Start', icon: 'download', click: () => this.save(true)}
        ];

        this.clustersOptions = []
        if(this.clusters){
            for(let c of this.clusters){
                this.clustersOptions.push({value:c.id,label:c.name})
            }
        }        
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

    goDomainEdit() {
        this.$state.go('base.console.edit.advanced.models.model', {modelID:this.cache.domains[0]});
    }

    save(start) {
        if (this.$scope.ui.inputForm.$invalid)
            return this.IgniteFormUtils.triggerValidation(this.$scope.ui.inputForm, this.$scope);
        
        for(let task of this.cacheDataProvider){
            task.group = task.targetCluster
            if(this.clusters){
                var targetCluster = '';
                var sourceCluster = ''
                for(let c of this.clusters){
                    if(c.id == task.targetCluster){
                        targetCluster = c.name + '.'
                        task.group = c.name
                    }
                    if(c.id == task.sourceCluster){
                        sourceCluster = c.name + '.'
                    }
                }
                task.name = 'Data from ' + sourceCluster + task.source + ' to ' + targetCluster + task.target                
            }
                
            let stat = this.TaskFlows.saveBasic(task).
              then((d)=>{
                if(d.data && d.data.message){
                    this.$scope.message = d.data.message;
                }
                else{
                    this.$scope.message = '成功保存任务！'
                }
             })
             .catch((d)=>{
                if(d.data && d.data.message){
                    this.$scope.message = d.data.message;
                }
            }); 
        }
            
        this.onSave({$event: {cache: cloneDeep(this.clonedCache), start}});
    }

    reset = (forReal) => forReal ? this.clonedCache = cloneDeep(this.cache) : void 0;

    confirmAndReset() {
        return this.IgniteConfirm.confirm('Are you sure you want to undo all changes for current cache?')
        .then(this.reset);
    }

    clearImplementationVersion(storeFactory) {
        delete storeFactory.implementationVersion;
    }    
    
    addCache() {
        const newFlow = this.TaskFlows.getBlankTaskFlow()
        newFlow.target = this.cache.name
        newFlow.targetCluster = this.clusterId                
        this.cacheDataProvider.push(newFlow);
    }
    
    removeCache(task) {
        if(task.id){
            let stat = from(this.TaskFlows.removeTaskFlow(task.group,task.id)).pipe(
                switchMap(({data}) => of(
                    {type: 'DELETE_TASK_FLOW_OK'}
                )),
                catchError((error) => of({
                    type: 'DELETE_TASK_FLOW_ERR',
                    error: {
                        message: `Failed to remove cluster task flow: ${error.data.message}.`
                    }
                }))
            );         
            stat.subscribe((d)=>{
                if(d.error){
                    this.$scope.message = d.error.message;
                }
            });
        }        
        const index = this.cacheDataProvider.indexOf(task, 0);
        if (index > -1) {
            this.cacheDataProvider.splice(index, 1);
        }        
    }
    
    changeCache(task) {
        let editFlow = this.cacheDataProvider.filter((item) => item.id == task.id )
        Object.assign(editFlow[0], task)
        console.log(task)
    }
}
