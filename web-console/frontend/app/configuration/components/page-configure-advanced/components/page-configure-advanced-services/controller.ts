

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
} from '../../../../store/actionCreators';

import {Confirm} from 'app/services/Confirm.service';
import {Menu} from 'app/types';
import ConfigureState from '../../../../services/ConfigureState';
import ConfigSelectors from '../../../../store/selectors';
import Caches from '../../../../services/Caches';
import Clusters from '../../../../services/Clusters';
import IgniteVersion from 'app/services/Version.service';
import ConfigChangesGuard from '../../../../services/ConfigChangesGuard';
import {UIRouter} from '@uirouter/angularjs';
import FormUtils from 'app/services/FormUtils.service';
import AgentManager from 'app/modules/agent/AgentManager.service';
import ConfigurationDownload from '../../../../services/ConfigurationDownload';

export default class PageConfigureAdvancedServicesController {
    form: ng.IFormController;
    cachesMenu: Menu<string>;
    servicesCachesMenu: Menu<string>;

    static $inject = [
        'Confirm', '$uiRouter', 'ConfigureState', 'ConfigSelectors', 'Clusters', 'Caches', 'IgniteVersion', '$element', 'ConfigChangesGuard', 'IgniteFormUtils', 'AgentManager', 'ConfigurationDownload', '$scope'
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
        private ConfigChangesGuard: ConfigChangesGuard<any>,
        private IgniteFormUtils: ReturnType<typeof FormUtils>,
        private AgentManager: AgentManager,
        private ConfigurationDownload: ConfigurationDownload,
        private $scope: ng.IScope
    ) {}
    
    $onDestroy() {
        this.subscription.unsubscribe();
        if (this.onBeforeTransition) this.onBeforeTransition();
        this.$element = null;
    }

    $postLink() {
        this.$element.addClass('panel--ignite');
    }

    _uiCanExit($transition$) {
        const options = $transition$.options();

        if (options.custom.justIDUpdate || options.redirectedFrom)
            return true;

        $transition$.onSuccess({}, () => this.reset());

        return forkJoin(
            this.ConfigureState.state$.pipe(pluck('edit', 'changes'), take(1)),
            this.clusterID$.pipe(
                switchMap((id) => this.ConfigureState.state$.pipe(this.ConfigSelectors.selectClusterShortCaches(id))),
                take(1)
            ),
            this.shortCaches$.pipe(take(1))
        )
            .toPromise()
            .then(([changes, originalShortCaches, currentCaches]) => {
                return this.ConfigChangesGuard.guard(
                    {
                        cluster: this.Clusters.normalize(this.originalCluster),
                        caches: originalShortCaches.map(this.Caches.normalize)
                    },
                    {
                        cluster: {...this.Clusters.normalize(this.clonedCluster), caches: changes.caches.ids},
                        caches: currentCaches.map(this.Caches.normalize)
                    }
                );
            });
    }

    $onInit() {
        this.onBeforeTransition = this.$uiRouter.transitionService.onBefore({}, (t) => this._uiCanExit(t));

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
                tap((v) => {
                    this.shortCaches = v;
                    this.cachesMenu = (this.shortCaches || []).map((c) => ({label: c.name, value: c.id}));
                    this.servicesCachesMenu = [{label: 'Key-affinity not used', value: null}].concat(this.cachesMenu);
                })
            ),           
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
                
            }))
        ).subscribe();

        this.formActionsMenu = [
            {
                text: 'Save',
                click: () => this.save(),
                icon: 'checkmark'
            },
            {
                text: 'Save and Download',
                click: () => this.save(true),
                icon: 'download'
            },
            {
                text: 'Deploy and ReStart',
                click: () => this.deploy(true),
                icon: 'refresh'
            },
            {
                text: 'Deploy Only',
                click: () => this.deploy(false),
                icon: 'expand'
            }
        ];

        this.$scope.ui = this.IgniteFormUtils.formUI();
        this.$scope.ui.loadedPanels = ['serviceConfigurations']; 
    }

    addCache() {
        this.ConfigureState.dispatchAction({type: 'ADD_CACHE_TO_EDIT'});
    }

    removeCache(cache) {
        this.ConfigureState.dispatchAction(
            removeClusterItems(this.$uiRouter.globals.params.clusterID, 'caches', [cache.id], false, false)
        );
    }

    changeCache(cache) {
        return this.ConfigureState.dispatchAction(changeItem('caches', cache));
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
    
    deploy(restart) {
        this.ConfigurationDownload.generateDownloadData(this.clonedCluster)
        .then((data)=>{
            var cfg = Object.assign({}, this.clonedCluster, {'blob': data, 'restart': restart});
            this.AgentManager.startCluster(cfg).then((msg) => {  
                if(!msg.message){
                   this.$scope.status = msg.status;
                   this.ConfigureState.dispatchAction({type: 'RESTART_CLUSTER'});
                   this.clonedCluster.status = msg.status;
                }            
                this.$scope.message = msg.message;
     
            })
        })
       .catch((e) => {
            this.$scope.message = ('Failed to generate project config file: '+e);           
        });
    }

    confirmAndReset() {
        return this.Confirm.confirm('Are you sure you want to undo all changes for current cluster?')
            .then(() => this.reset())
            .catch(() => {});
    }    
}
