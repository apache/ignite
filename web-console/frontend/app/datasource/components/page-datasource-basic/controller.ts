

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
        // if (this.onBeforeTransition) this.onBeforeTransition();
        this.$element = null;
    }

    $postLink() {
        this.$element.addClass('panel--ignite');
    }

    $onInit() {
        const $scope = this.$scope;
        // this.onBeforeTransition = this.$uiRouter.transitionService.onBefore({}, (t) => this._uiCanExit(t));
        this.available = (v) =>{ return true; }
        
        let drivers = [];
        for(let engine of dbPresets){
            let option = {"label": engine.db, "value": engine.driverCls}
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
            $scope.selectedPreset = this.clonedCluster
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

        $scope.selectedPreset = {
            db: 'Generic',            
            driverCls: '',
            jdbcUrl: 'jdbc:[database]',
            user: 'sa',
            password: ''            
        };

        this._loadPresets();

        $scope.$watch('selectedPreset.driverCls', (idx) => {
            const val = $scope.selectedPreset.driverCls;

            if (val && !(this.clonedCluster.jndiName)) {
                const foundPreset = this._findPreset(val);
                const selectedPreset = $scope.selectedPreset;
                selectedPreset.db = foundPreset.db;
                selectedPreset.jdbcUrl = foundPreset.jdbcUrl;
                selectedPreset.user = foundPreset.user;                
            }
        }, true);
       
    }

    _loadPresets() {
        try {
            const _dbPresets = dbPresets
            const restoredPresets = JSON.parse(localStorage.dbPresets);

            _.forEach(restoredPresets, (restoredPreset) => {
                const preset = _.find(_dbPresets, {driverCls: restoredPreset.driverCls});

                if (preset) {
                    preset.jdbcUrl = restoredPreset.jdbcUrl;
                    preset.user = restoredPreset.user;
                }
            });
        }
        catch (ignored) {
            // No-op.
        }
    }    

    _savePreset(preset) {
        try {
            const _dbPresets = dbPresets
            const oldPreset = _.find(_dbPresets, {driverCls: preset.driverCls});

            if (oldPreset){          
                oldPreset.jdbcUrl = preset.jdbcUrl;
                oldPreset.user = preset.user;
            }                
            else{
                _dbPresets.push(preset);
            }
                

            localStorage.dbPresets = JSON.stringify(_dbPresets);
        }
        catch (err) {
            this.$scope.message = err.toString();
        }
    }

    _findPreset(selectedJdbcCls) {
        const _dbPresets = dbPresets
        let result = _.find(_dbPresets, function(preset) {
            return preset.driverCls === selectedJdbcCls;
        });

        if (!result){
            result = {db: 'Generic', jdbcUrl: 'jdbc:[database]', user: 'admin'};        
            result.driverCls = selectedJdbcCls;
        }            
        return result;
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
            this._savePreset(datasource);
            const foundPreset = this._findPreset(datasource.driverCls);
            datasource.db = foundPreset.db
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
            stat.subscribe(
                (next) => {
                    this.$scope.message = 'Save successful.'
                    if(redirect){                
                        return this.$uiRouter.stateService.go('base.datasource.overview');
                    }
                }
            );  
            
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
