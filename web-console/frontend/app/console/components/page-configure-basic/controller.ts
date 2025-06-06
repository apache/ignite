

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
import LegacyUtilsFactory from 'app/services/LegacyUtils.service';
import {UIRouter} from '@uirouter/angularjs';
import FormUtils from 'app/services/FormUtils.service';
import AgentManager from 'app/modules/agent/AgentManager.service';
import NodeMetrics from 'app/modules/cluster/NodeMetrics';



export default class PageConfigureBasicController {
    form: ng.IFormController;

    static $inject = [
        'Confirm', '$uiRouter', 'ConfigureState', 'ConfigSelectors', 'Clusters', 'Caches', 'IgniteLegacyUtils', '$element', 'IgniteFormUtils', 'AgentManager', '$scope'
    ];

    constructor(
        private Confirm: Confirm,
        private $uiRouter: UIRouter,
        private ConfigureState: ConfigureState,
        private ConfigSelectors: ConfigSelectors,
        private Clusters: Clusters,
        private Caches: Caches,
        private LegacyUtils: ReturnType<typeof LegacyUtilsFactory>,
        private $element: JQLite,        
        private IgniteFormUtils: ReturnType<typeof FormUtils>,
        private AgentManager: AgentManager,      
        private $scope: ng.IScope
    ) {

        this.formActionsMenu.push({
            text: 'Ctl Cluster',
            click: () => this.callService('CacheMetricsService'),
            icon: 'checkmark'
        })
        
        this.$scope.formActionsMenu = this.formActionsMenu;        
        this.$scope.currentNode = -1;
    }
    
    formActionsMenu:Array<any> = [];
    kvColumnDefs = [
         {
            name: 'name',
            displayName: 'Name',
            field: 'name',
            enableHiding: false,
            filter: {
                placeholder: 'Filter by name…'
            },
            sort: {direction: 'asc', priority: 0},
            sortingAlgorithm: naturalCompare,                        
            minWidth: 200
        },
        {
            name: 'value',
            displayName: 'Value',
            field: 'value',
            enableHiding: false,
            enableFiltering: false,
            minWidth: 200
        },
    ]; 

    clusterMetrics = [];
    nodesAttrs = [];
    nodeList = [];
    nodeAttrs = [];

    $onDestroy() {
        this.subscription.unsubscribe();        
        this.$element = null;
    }

    $postLink() {
        this.$element.addClass('panel--ignite');
    }

    $onInit() {

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
                
                this.clonedCluster.demo = this.AgentManager.isDemoMode();
                
                this.originalCluster.status =  this.clonedCluster.status;
                
                this.callService('ClusterInfoService').then((m)=>{
                    if(m){
                        this.clusterMetrics = this.LegacyUtils.objectToKvList(m.metrics);
                        this.nodesAttrs = m.nodes;
                        this.nodeList = this._nodeList();                        
                        this.onNodeSelect(0);                    
                    } 
                });
                
            }))
        ).subscribe();

        this.$scope.ui = this.IgniteFormUtils.formUI();
        this.$scope.ui.loadedPanels = [];        

    }    

    _nodeList() {
        let options = [];
        for(let i=0;i<this.nodesAttrs.length;i++){
            let port = this.nodesAttrs[i]['org.apache.ignite.rest.tcp.port']
            options.push({
                value: i,
                label: i+'. '+this.nodesAttrs[i]['node.addresses']+':'+port
            });
        };
        return options;
    }

    onNodeSelect(currentNode) {
        this.$scope.currentNode = currentNode;
        let node = this.nodesAttrs[currentNode];
        if (node) {
            this.nodeAttrs = this.LegacyUtils.objectToKvList(node);            
            this.$scope.$applyAsync();
        }
    }

    reset() {
        this.clonedCluster = cloneDeep(this.originalCluster);
        this.ConfigureState.dispatchAction({type: 'RESET_EDIT_CHANGES'});
    }
    
    buildFormActionsMenu(){
        let formActionsMenu = [];
       
        if(this.$scope.status && this.$scope.status=="stoped"){
            formActionsMenu.push({
                text: 'Start Cluster',
                click: () => this.confirmAndStart(false),
                icon: 'checkmark'
            });
            formActionsMenu.push({
                text: 'Start Crud UI',
                click: () => this.confirmAndStart(true),
                icon: 'checkmark'
            })
        }        
        else {
            formActionsMenu.push({
                text: 'Start Cluster',
                click: () => this.confirmAndStart(false),
                icon: 'checkmark'
            });
            formActionsMenu.push({
                text: 'Start Crud UI',
                click: () => this.confirmAndStart(true),
                icon: 'checkmark'
            })
            formActionsMenu.push({
                text: 'Stop Cluster',
                click: () => this.confirmAndStop(),
                icon: 'exit'
            });
            formActionsMenu.push({
                text: 'Restart Cluster',
                click: () => this.confirmAndRestart(),
                icon: 'refresh'
            })
        }
        this.$scope.formActionsMenu = formActionsMenu;
        return formActionsMenu;
    }    
    
    start(restart:boolean) {
        this.clonedCluster['restart'] = restart;
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

    startWithUI(restart:boolean) {
        this.clonedCluster['restart'] = restart;
        this.clonedCluster['crudui'] = true;
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
    
    callService(serviceName,args={}) {
        return this.AgentManager.callClusterService(this.clonedCluster,serviceName,args).then((data) => {  
            this.$scope.status = data.status;
            this.buildFormActionsMenu();
            this.clonedCluster.status = data.status;            
            if(data.message){
                this.$scope.message = data.message;
            }
            if(data.result){
                return data.result;
            }
            return {}
        })   
       .catch((e) => {
           this.$scope.status = 'stoped'
           this.buildFormActionsMenu();
           this.$scope.message = ('Failed to callClusterService : '+serviceName+' Caused : '+e.message);           
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
    confirmAndStart(withUI:boolean) {
        return this.Confirm.confirm('Are you sure you want to start current cluster? Current status:' + this.clonedCluster.status)
            .then(() => {
                if(withUI) {
                    this.startWithUI(false);
                }
                else{
                    this.start(false);
                }

            })
            .catch(() => {});
    }
    confirmAndRestart() {
        return this.Confirm.confirm('Are you sure you want to restart current cluster? Current status:' + this.clonedCluster.status)
            .then(() => this.start(true))
            .catch(() => {});
    }
    confirmAndStop() {
        return this.Confirm.confirm('Are you sure you want to stop current cluster?  Current status:' + this.clonedCluster.status)
            .then(() => this.stop())
            .catch(() => {});
    }
}
