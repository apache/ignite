
import {forkJoin, merge, from, of} from 'rxjs';
import {map, tap, pluck, take, filter, catchError, distinctUntilChanged, switchMap, publishReplay, refCount} from 'rxjs/operators';
import cloneDeep from 'lodash/cloneDeep';

import {Confirm} from 'app/services/Confirm.service';

import ConfigureState from 'app/configuration/services/ConfigureState';
import {UIRouter} from '@uirouter/angularjs';
import FormUtils from 'app/services/FormUtils.service';
import AgentManager from 'app/modules/agent/AgentManager.service';


export default class PageIgfsAdvancedController {    

    static $inject = [
        'Confirm', '$uiRouter', 'ConfigureState', '$element', 'IgniteFormUtils', 'AgentManager', '$scope'
    ];

    form: ng.IFormController;

    onBeforeTransition: CallableFunction;
    
    constructor(
        private Confirm: Confirm,
        private $uiRouter: UIRouter,
        private ConfigureState: ConfigureState,        
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
        const $scope = this.$scope;
        this.onBeforeTransition = this.$uiRouter.transitionService.onBefore({}, (t) => this._uiCanExit(t));
        
        const clusterID$ = this.$uiRouter.globals.params$.pipe(
            take(1),
            pluck('storageID'),
            filter((v) => v),
            take(1)
        );
        this.clusterID$ = clusterID$;

        this.isNew$ = this.$uiRouter.globals.params$.pipe(pluck('storageID'), map((id) => id === 'new'));
        
        
        this.originalCluster$ = clusterID$.pipe(
            distinctUntilChanged(),
            switchMap((id) => {
                return from(of(this._loadMongoExpress(id)));
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
               click: () => this.save(true),
               icon: 'checkmark'
           },
           {
               text: 'Delete',
               click: () => this.confirmAndDelete(),
               icon: 'download'
           }           
        ];        
       
    }
    
    _uiCanExit($transition$) {
        const options = $transition$.options();

        if (options.custom.justIDUpdate || options.redirectedFrom)
            return true;

        $transition$.onSuccess({}, () => this.reset());

        return true;
    }

    _loadMongoExpress(id: string) {
        try {            
            const mongoExpress = JSON.parse(localStorage.igfsStorages);
            if (mongoExpress && mongoExpress[id]) {            
                return mongoExpress[id];
            }
        }
        catch (ignored) {
            
        }
        return {id: id, comment: "Ignite FileSystem", url: ""}
    }

    _saveMongoExpress(preset) {
       
        let mongoExpress = localStorage.igfsStorages;
        if (!mongoExpress) {            
            mongoExpress = {}
        }
        else{
            mongoExpress = JSON.parse(localStorage.igfsStorages)
        }
        mongoExpress[preset.id] = preset;
        localStorage.igfsStorages = JSON.stringify(mongoExpress);        
    }

    _removeMongoExpress(id: string) {
        const mongoExpress = JSON.parse(localStorage.igfsStorages);
        if (mongoExpress) {            
            delete mongoExpress[id];
        }
        localStorage.igfsStorages = JSON.stringify(mongoExpress);
    }

    save(redirect = false) {
        if (this.form.$invalid)
            return this.IgniteFormUtils.triggerValidation(this.form, this.$scope);
        let datasource = this.clonedCluster;
        if(datasource) {
            try {
                this._saveMongoExpress(datasource);                   
                this.$scope.message = 'Save successful.';
                if(redirect){                
                    setTimeout(() => {
                        this.$uiRouter.stateService.go('base.igfs.overview');
                    },100)
                }
            }
            catch (err) {
                this.$scope.message = err.toString();
            }         
        }        
    }

    reset() {
        this.clonedCluster = cloneDeep(this.originalCluster);
        this.ConfigureState.dispatchAction({type: 'RESET_EDIT_CHANGES'});
    }
    
    delete(datasource) {
        this._removeMongoExpress(datasource.id)   
    }
    
    confirmAndDelete() {
        return this.Confirm.confirm('Are you sure you want to delete current storage?')
            .then(() => this.delete(this.clonedCluster))
            .catch(() => {});
    }
}
