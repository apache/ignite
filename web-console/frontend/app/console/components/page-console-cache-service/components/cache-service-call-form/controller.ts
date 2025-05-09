

import cloneDeep from 'lodash/cloneDeep';
import get from 'lodash/get';
import {Subject, BehaviorSubject, merge, combineLatest, from, of, empty} from 'rxjs';
import {tap,map,filter} from 'rxjs/operators';
import {Menu} from 'app/types';

import LegacyConfirmFactory from 'app/services/Confirm.service';
import Version from 'app/services/Version.service';
import Caches from 'app/configuration/services/Caches';
import FormUtilsFactory from 'app/services/FormUtils.service';
import AgentManager from 'app/modules/agent/AgentManager.service';

export default class CacheServiceCallFormController {
    modelsMenu: Menu<string>;

    onCall: ng.ICompiledExpression;
    
    clusterId: string;

    static $inject = ['IgniteConfirm', 'IgniteVersion', '$scope', 'Caches', 'IgniteFormUtils', 'AgentManager'];

    constructor(
        private IgniteConfirm: ReturnType<typeof LegacyConfirmFactory>,
        private IgniteVersion: Version,
        private $scope: ng.IScope,
        private Caches: Caches,
        private IgniteFormUtils: ReturnType<typeof FormUtilsFactory>,
        private AgentManager: AgentManager
    ) {}

    $onInit() {
        this.available = this.IgniteVersion.available.bind(this.IgniteVersion);

        this.$scope.ui = this.IgniteFormUtils.formUI();

        this.$scope.formActions = [   
            {text: 'Choose Service:', icon: 'plus', click: () => this.updateServices()},
        ];        
        setTimeout(() => this.updateServices(), 2000);
    }

    $onDestroy() {
       
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

    
    callServiceForCache(serviceName:string) {
        let params = this.clonedCache;
        let args = this.onCall({$event: {cache: this.clonedCache}});
        let clusterId = args['clusterId'];
        params = Object.assign(args,params);
        this.$scope.message = 'Calling service ...';
        this.AgentManager.callCacheService({id: clusterId},serviceName,args).then((data) => {  
            this.$scope.status = data.status;
            if(data.message){
                this.$scope.message = data.message;                
            }
            if(data.result){
                return data.result;
            }
            return {}
        })   
       .catch((e) => {
            this.$scope.message = ('Failed to callClusterService : '+serviceName+' Caused : '+e);                    
        });
    }

    reset = (forReal) => forReal ? this.clonedCache = cloneDeep(this.cache) : void 0;

    updateServices(){        
        if(this.services){
            let formActions = []
            for(let service of this.services){
                let action = {
                    text: service.description,
                    icon: 'checkmark',
                    click:  () => this.confirmAndCall(service)
                };            
                formActions.push(action);
            }                
            this.$scope.formActions = formActions;
        }
        else{
            setTimeout(() => this.updateServices(), 2000);
        }       
    }
    
    confirmAndCall(service) {        
        if (this.$scope.ui.inputForm && this.$scope.ui.inputForm.$invalid)
            return this.IgniteFormUtils.triggerValidation(this.$scope.ui.inputForm, this.$scope);
        return this.IgniteConfirm.confirm('Are you sure you want to '+service.description+' for current selected caches?')
            .then( () => { this.callServiceForCache(service.name); } );
    }

    clearImplementationVersion(storeFactory) {
        delete storeFactory.implementationVersion;
    }
}
