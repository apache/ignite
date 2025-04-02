
import {forkJoin, merge, from, of} from 'rxjs';
import {map, tap, pluck, take, filter, catchError, distinctUntilChanged, switchMap, publishReplay, refCount} from 'rxjs/operators';
import cloneDeep from 'lodash/cloneDeep';

import {Confirm} from 'app/services/Confirm.service';

import ConfigureState from 'app/configuration/services/ConfigureState';
import {UIRouter} from '@uirouter/angularjs';
import FormUtils from 'app/services/FormUtils.service';
import Datasource from 'app/datasource/services/Datasource';
import {DatasourceDto} from 'app/configuration/types';

export default class PageDatasetsAdvancedController {    

    static $inject = [
        'Confirm', '$uiRouter', 'ConfigureState', '$element', 'IgniteFormUtils', 'Datasource', '$scope'
    ];

    form: ng.IFormController;
    
    onBeforeTransition: CallableFunction;

    baseUrl = 'http://localhost:3000';

    constructor(
        private Confirm: Confirm,
        private $uiRouter: UIRouter,
        private ConfigureState: ConfigureState,        
        private $element: JQLite,        
        private IgniteFormUtils: ReturnType<typeof FormUtils>,
        private Datasource: Datasource, 
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
        
        const datasetID$ = this.$uiRouter.globals.params$.pipe(
            take(1),
            pluck('datasetID'),
            filter((v) => v),
            take(1)
        );
        
        this.originalDatasource$ = datasetID$.pipe(
            distinctUntilChanged(),
            switchMap((id) => {
                return from(this.Datasource.selectDatasource(id));
            }),
            distinctUntilChanged(),
            publishReplay(1),
            refCount()
        );  
        
        this.originalDatasource$.subscribe((c) =>{
            this.originalDatasource = this._loadMongoExpress(c);
            this.clonedDatasource = this._loadMongoExpress(c);
            
        })
       
        this.formActionsMenu = [
           {
               text: 'Save',
               click: () => this.save(true),
               icon: 'checkmark'
           },
           {
               text: 'Reset',
               click: () => this.reset(),
               icon: 'refresh'
           }           
        ];        
       
    }
    
    _uiCanExit($transition$) {
        const options = $transition$.options();

        if (options.custom.justIDUpdate || options.redirectedFrom)
            return true;
        return true;
    }

    _loadMongoExpress(dto: DatasourceDto) {
        if(!dto.jdbcProp['web_url']){
            dto.jdbcProp['web_url'] = this.baseUrl+"/webapps/mongoAdmin/queryDocuments#"+dto.jndiName
        }   
        return dto;
    }

    _saveMongoExpress(datasource) {
       
        let stat = from(this.Datasource.saveAdvanced(datasource)).pipe(
            switchMap(({data}) => of(
                {type: 'SAVE_AND_EDIT_DATASOURCE_OK'}
            )),
            catchError((error) => of({
                type: 'SAVE_AND_EDIT_DATASOURCE_ERR',
                error: {
                    message: `Failed to save datasource : ${error.data.message}.`
                }
            }))
        );
        return stat;        
    }    

    save(redirect = false) {
        if (this.form.$invalid)
            return this.IgniteFormUtils.triggerValidation(this.form, this.$scope);
        let datasource = this.clonedDatasource;
        if(datasource) {
            try{
                this._saveMongoExpress(datasource);                   
                this.$scope.message = 'Save successful.';
                if(redirect){                
                    setTimeout(() => {
                        this.$uiRouter.stateService.go('base.datasets.overview');
                    },100)
                }
            }
            catch (err) {
                this.$scope.message = err.toString();
            }                  
        }        
    }

    reset() {
        this.clonedDatasource = cloneDeep(this.originalDatasource);
        this.ConfigureState.dispatchAction({type: 'RESET_EDIT_CHANGES'});
    }
}
