import cloneDeep from 'lodash/cloneDeep';
import uuidv4 from 'uuid/v4';
import {Subject, BehaviorSubject, Observable, combineLatest, merge} from 'rxjs';
import {pluck, tap, skip, publishReplay, refCount, distinctUntilChanged, switchMap, map} from 'rxjs/operators';
import get from 'lodash/get';

import hasIndexTemplate from './hasIndex.template.pug';
import keyCellTemplate from './keyCell.template.pug';
import valueCellTemplate from './valueCell.template.pug';
import {removeClusterItems, advancedSaveModel} from 'app/configuration/store/actionCreators';
import {default as ConfigSelectors} from 'app/configuration/store/selectors';
import {default as ConfigureState} from 'app/configuration/services/ConfigureState';
import {default as Models} from 'app/configuration/services/Models';
import AgentManager from 'app/modules/agent/AgentManager.service';
import {UIRouter, StateService} from '@uirouter/angularjs';
import {Confirm} from 'app/services/Confirm.service';
import {ShortDomainModel, DomainModel, ShortCache} from 'app/configuration/types';
import {IColumnDefOf} from 'ui-grid';
import ConfigSelectionManager from 'app/configuration/services/ConfigSelectionManager';

export default class PageConfigureAdvancedModels {
    static $inject = ['ConfigSelectors', 'ConfigureState', 'Confirm', '$scope', '$uiRouter', 'Models', '$state', 'AgentManager', 'configSelectionManager'];

    constructor(
        private ConfigSelectors: ConfigSelectors,
        private ConfigureState: ConfigureState,
        private Confirm: Confirm,
        private $scope: ng.IScope,
        private $uiRouter: UIRouter,
        private Models: Models,
        private $state: StateService,
        private AgentManager: AgentManager, 
        private configSelectionManager: ReturnType<typeof ConfigSelectionManager>
    ) {}
    
    visibleRows$ = new Subject<Array<ShortDomainModel>>();
    selectedRows$ = new BehaviorSubject<Array<ShortDomainModel>>([]); // modiy@byron new Subject();

    columnDefs: Array<IColumnDefOf<ShortDomainModel>>;
    itemID$: Observable<string>;
    shortItems$: Observable<Array<ShortDomainModel>>;
    shortCaches$: Observable<Array<ShortCache>>;
    originalItem$: Observable<DomainModel>;

    $onDestroy() {
        this.subscription.unsubscribe();
        this.visibleRows$.complete();
        this.selectedRows$.complete();
    }
    $onInit() { 
        this.columnDefs = [
            {
                name: 'hasIndex',
                displayName: 'Indexed',
                field: 'hasIndex',
                type: 'boolean',
                enableFiltering: true,
                visible: true,
                multiselectFilterOptions: [{value: true, label: 'Yes'}, {value: false, label: 'No'}],
                width: 100,
                cellTemplate: hasIndexTemplate
            },
            {
                name: 'keyType',
                displayName: 'Key type',
                field: 'keyType',
                enableHiding: false,
                filter: {
                    placeholder: 'Filter by key type…'
                },
                cellTemplate: keyCellTemplate,
                width: 165
            },
            {
                name: 'valueType',
                displayName: 'Value type',
                field: 'valueType',
                enableHiding: false,
                filter: {
                    placeholder: 'Filter by value type…'
                },
                sort: {direction: 'asc', priority: 0},
                cellTemplate: valueCellTemplate,
                minWidth: 200,
                maxWidth: 350
            },
            {
                name: 'valueLabel',
                displayName: 'Value label',
                field: 'tableComment',
                enableHiding: false,
                filter: {
                    placeholder: 'Filter by value label'
                },
                sort: {direction: 'asc', priority: 0},
                cellTemplate: valueCellTemplate,
                minWidth: 200
            }
        ];

        this.itemID$ = this.$uiRouter.globals.params$.pipe(pluck('modelID'));

        this.shortItems$ = this.ConfigureState.state$.pipe(
            this.ConfigSelectors.selectCurrentShortModels,
            tap((shortModels = []) => {
                const value = shortModels.every((m) => m.hasIndex);
                this.columnDefs[0].visible = !value;
            }),
            publishReplay(1),
            refCount()
        );

        this.shortCaches$ = this.ConfigureState.state$.pipe(this.ConfigSelectors.selectCurrentShortCaches);

        this.originalItem$ = this.itemID$.pipe(
            distinctUntilChanged(),
            switchMap((id) => {
                return this.ConfigureState.state$.pipe(this.ConfigSelectors.selectModelToEdit(id));
            }),
            distinctUntilChanged(),
            publishReplay(1),
            refCount()
        );

        this.isNew$ = this.itemID$.pipe(map((id) => id === 'new'));

        this.itemEditTitle$ = combineLatest(this.isNew$, this.originalItem$, (isNew, item) => {
            return `${isNew ? 'Create' : 'Edit'} model ${!isNew && get(item, 'valueType') ? `‘${get(item, 'valueType')}’` : ''}`;
        });
        
        // 将 BehaviorSubject 转换为忽略初始值的 Observable
        this.selectionManager = this.configSelectionManager({
            itemID$: this.itemID$,
            selectedItemRows$: this.selectedRows$.pipe(skip(1)),
            visibleRows$: this.visibleRows$,
            loadedItems$: this.shortItems$
        });

        this.tableActions$ = this.selectionManager.selectedItemIDs$.pipe(map((selectedItems:Array<string>) => [
            {
                action: 'Clone',
                click: () => this.clone(selectedItems),
                available: selectedItems.length==1
            },
            {
                action: 'Delete',
                click: () => {
                    this.remove(selectedItems);
                },
                available: true
            }
        ]));

        this.subscription = merge(
            this.originalItem$,
            this.selectionManager.editGoes$.pipe(tap((id) => this.edit(id))),
            this.selectionManager.editLeaves$.pipe(tap((options) => this.$state.go('base.console.edit.advanced.models', null, options)))
        ).subscribe();

        this.openDrawer = false;
        
    }    

    edit(modelID) {
        this.$state.go('base.console.edit.advanced.models.model', {modelID});
    }

    save({model, download}) {
        this.ConfigureState.dispatchAction(advancedSaveModel(model, download));
    }

    clone(itemIDs: Array<string>) {
        this.originalItem$.pipe(            
            switchMap((cache) => {
                let clonedCache = cloneDeep(cache);
                clonedCache.id = uuidv4();
                clonedCache.valueType = cache.valueType+'_cloned';
                this.ConfigureState.dispatchAction(
                    advancedSaveModel(clonedCache, false)
                );
                return clonedCache;
            })
        )
    }

    remove(itemIDs: Array<string>) {
        const serviceName = 'CacheDeleteTableService';
        const currentValue = this.selectedRows$.getValue();
        const args = {
            caches: currentValue.map((item) => item.valueType)
        }
        
        this.Confirm.confirm('Are you sure you want to destroy current selected models?').then(() => true)
            .then(() => {
                this.$scope.message = 'Deleting ...';
                this.AgentManager.callCacheService(this.$uiRouter.globals.params.clusterID,serviceName,args).then((data) => {  
                    this.$scope.status = data.status;                            
                    if(data.message){
                        this.$scope.message = data.message;
                    }
                })   
               .catch((e) => {
                   this.$scope.status = 'failed'                   
                   this.$scope.message = ('Failed to callClusterService : '+serviceName+' Caused : '+e);           
                });
            })
            .catch(() => {});

        this.ConfigureState.dispatchAction(
            removeClusterItems(this.$uiRouter.globals.params.clusterID, 'models', itemIDs, true, true)
        );        
    }
}
