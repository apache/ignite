
import {forkJoin, merge, from, of} from 'rxjs';
import {map, tap, pluck, take, filter, catchError, distinctUntilChanged, switchMap, publishReplay, refCount} from 'rxjs/operators';
import cloneDeep from 'lodash/cloneDeep';
import {advancedSaveCluster} from 'app/configuration/store/actionCreators';
import {Confirm} from 'app/services/Confirm.service';

import ConfigureState from 'app/configuration/services/ConfigureState';
import ConfigSelectors from '../../../configuration/store/selectors';
import {UIRouter} from '@uirouter/angularjs';
import FormUtils from 'app/services/FormUtils.service';
import Datasource from 'app/datasource/services/Datasource';
import {DatasourceDto} from 'app/configuration/types';
import {Cluster} from 'app/configuration/types';

export default class PageDatasetsAdvancedController {    

    static $inject = [
        'Confirm', '$uiRouter', 'ConfigureState', 'ConfigSelectors','$element', 'IgniteFormUtils', 'Datasource', '$scope'
    ];

    form: ng.IFormController;
    
    onBeforeTransition: CallableFunction;

    baseUrl = 'http://localhost:3000';

    constructor(
        private Confirm: Confirm,
        private $uiRouter: UIRouter,
        private ConfigureState: ConfigureState,
        private ConfigSelectors: ConfigSelectors,          
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

        this.originalCluster$ = datasetID$.pipe(
            distinctUntilChanged(),
            switchMap((id) => {
                return this.ConfigureState.state$.pipe(this.ConfigSelectors.selectClusterToEdit(id));
            }),
            distinctUntilChanged(),
            publishReplay(1),
            refCount()
        );
        
        this.originalCluster$.subscribe((c) =>{
            this.originalDatasource = cloneDeep(c);
            this.clonedDatasource = c;
            console.log(c);
            
        })
       
        this.formActionsMenu = [
           {
               text: 'Save',
               click: () => this.save(this.clonedDatasource),
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

    save({cluster}) {
        this.ConfigureState.dispatchAction(advancedSaveCluster(cluster, false));
    }
    reset() {
        this.clonedDatasource = cloneDeep(this.originalDatasource);
        this.ConfigureState.dispatchAction({type: 'RESET_EDIT_CHANGES'});
    }
}
