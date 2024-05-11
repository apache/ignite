

import {forkJoin, merge} from 'rxjs';
import {map, tap, pluck, take, filter, distinctUntilChanged, switchMap, publishReplay, refCount} from 'rxjs/operators';
import cloneDeep from 'lodash/cloneDeep';
import get from 'lodash/get';
import naturalCompare from 'natural-compare-lite';
import {
    changeItem,
    removeClusterItems,
    basicSave,
    basicSaveAndDownload
} from 'app/configuration/store/actionCreators';

import {Confirm} from 'app/services/Confirm.service';

import ConfigureState from '../../../configuration/services/ConfigureState';
import ConfigSelectors from '../../../configuration/store/selectors';
import Caches from '../../../configuration/services/Caches';
import Clusters from '../../../configuration/services/Clusters';
import IgniteVersion from 'app/services/Version.service';
import {UIRouter} from '@uirouter/angularjs';
import FormUtils from 'app/services/FormUtils.service';
import AgentManager from 'app/modules/agent/AgentManager.service';


export default class PageConfigureBasicController {
    form: ng.IFormController;

    static $inject = [
        'Confirm', '$uiRouter', 'ConfigureState', 'ConfigSelectors', 'Clusters', 'Caches', 'IgniteVersion', '$element', 'IgniteFormUtils', 'AgentManager', '$scope'
    ];

    constructor(
        private Confirm: Confirm,
        private $uiRouter: UIRouter,
        private ConfigureState: ConfigureState,
        private ConfigSelectors: ConfigSelectors,
        private Clusters: Clusters,
        private Caches: Caches,
        private IgniteVersion: IgniteVersion,
        private $element: JQLite,        
        private IgniteFormUtils: ReturnType<typeof FormUtils>,
        private AgentManager: AgentManager,      
        private $scope: ng.IScope
    ) {
        this.formActionsMenu = []
        this.formActionsMenu.push({
            text: 'Ctl Cluster',
            click: () => this.callService('serviceList'),
            icon: 'checkmark'
        })
        
        this.$scope.formActionsMenu = this.formActionsMenu;
    }
    
    $onDestroy() {
        this.subscription.unsubscribe();        
        this.$element = null;
    }

    $postLink() {
        this.$element.addClass('panel--ignite');
    }

    $onInit() {        

        this.memorySizeInputVisible$ = this.IgniteVersion.currentSbj.pipe(
            map((version) => this.IgniteVersion.since(version.ignite, '2.0.0'))
        );

        const clusterID$ = this.$uiRouter.globals.params$.pipe(
            take(1),
            pluck('clusterID'),
            filter((v) => v),
            take(1)
        );
        this.clusterID$ = clusterID$;

        this.isNew$ = this.$uiRouter.globals.params$.pipe(pluck('clusterID'), map((id) => id === 'new'));
        this.shortCaches$ = this.ConfigureState.state$.pipe(this.ConfigSelectors.selectCurrentShortCaches);
        this.shortClusters$ = this.ConfigureState.state$.pipe(this.ConfigSelectors.selectShortClustersValue());
        
        this.originalCluster$ = clusterID$.pipe(
            distinctUntilChanged(),
            switchMap((id) => {
                return this.ConfigureState.state$.pipe(this.ConfigSelectors.selectClusterToEdit(id));
            }),
            distinctUntilChanged(),
            publishReplay(1),
            refCount()
        );       
        
        this.serviceList = {'status':{ name:'status',description:'get cluster last status'}};
        
        this.subscription = merge(
            this.shortCaches$.pipe(
                map((caches) => caches.sort((a, b) => naturalCompare(a.name, b.name))),
                tap((v) => this.shortCaches = v)
            ),
            this.shortClusters$.pipe(tap((v) => this.shortClusters = v)),
            this.originalCluster$.pipe(tap((v) => {
                this.originalCluster = v;
                // clonedCluster should be set only when particular cluster edit starts.
                // 
                // Stored cluster changes should not propagate to clonedCluster because it's assumed
                // that last saved copy has same shape to what's already loaded. If stored cluster would overwrite
                // clonedCluster every time, then data rollback on server errors would undo all changes
                // made by user and we don't want that. Advanced configuration forms do the same too.
                if (get(v, 'id') !== get(this.clonedCluster, 'id')) this.clonedCluster = cloneDeep(v);
                                
                this.serviceList = Object.assign({},this.callService('serviceList'));  
                
                this.clonedCluster.demo = this.AgentManager.isDemoMode();
                
                this.originalCluster.status =  this.clonedCluster.status;
                
                
            }))
        ).subscribe();  

        this.cachesColDefs = [
            {name: 'Name:', cellClass: 'pc-form-grid-col-20'},
            {name: 'Mode:', cellClass: 'pc-form-grid-col-10'},
            {name: 'Atomicity:', cellClass: 'pc-form-grid-col-10', tip: `
                Atomicity:
                <ul>
                    <li>ATOMIC - in this mode distributed transactions and distributed locking are not supported</li>
                    <li>TRANSACTIONAL - in this mode specified fully ACID-compliant transactional cache behavior</li>
                    <li>TRANSACTIONAL_SNAPSHOT - in this mode specified fully ACID-compliant transactional cache behavior for both key-value API and SQL transactions</li>
                </ul>
            `},
            {name: 'Backups:', cellClass: 'pc-form-grid-col-10', tip: `
                Number of nodes used to back up single partition for partitioned cache
            `}
        ]; 
        
    }

    changeCache(cache) {
        return this.ConfigureState.dispatchAction(changeItem('caches', cache));
    }

    save(download = false) {
        if (this.form.$invalid)
            return this.IgniteFormUtils.triggerValidation(this.form, this.$scope);

        // todo@byron
    }

    reset() {
        this.clonedCluster = cloneDeep(this.originalCluster);
        this.ConfigureState.dispatchAction({type: 'RESET_EDIT_CHANGES'});
    }
    
    buildFormActionsMenu(){
       let formActionsMenu = [];//this.$scope.formActionsMenu;
       //this.$scope.formActionsMenu.length = 0;
       
       if(this.$scope.status && this.$scope.status!="started"){
           formActionsMenu.push({
               text: 'Start Cluster',
               click: () => this.confirmAndRestart(),
               icon: 'checkmark'
           })
       }        
       else {
           formActionsMenu.push({
               text: 'Stop Cluster',
               click: () => this.confirmAndStop(),
               icon: 'download'
           });
           formActionsMenu.push({
               text: 'Restart Cluster',
               click: () => this.confirmAndRestart(),
               icon: 'checkmark'
           })
       }
       this.$scope.formActionsMenu = formActionsMenu;
       return formActionsMenu;
    }
    
    
    restart() {
        this.clonedCluster['restart'] = true;
        this.AgentManager.startCluster(this.clonedCluster).then((msg) => {  
            if(!msg.message){
               this.$scope.status = msg.status;
               this.ConfigureState.dispatchAction({type: 'START_CLUSTER'});
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
               this.ConfigureState.dispatchAction({type: 'STOP_CLUSTER'});
               this.clonedCluster.status = msg.status;
            }            
            this.$scope.message = msg.message;
        });        
    }
    
    callService(serviceName,args) {
        this.AgentManager.callClusterService(this.clonedCluster,serviceName,args).then((data) => {  
            this.$scope.status = data.status;
            this.buildFormActionsMenu();
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
           this.$scope.status = 'stoped'
           this.buildFormActionsMenu();
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
