

import cloneDeep from 'lodash/cloneDeep';
import get from 'lodash/get';
import {tap} from 'rxjs/operators';
import {Menu} from 'app/types';

import LegacyConfirmFactory from 'app/services/Confirm.service';
import Version from 'app/services/Version.service';
import Caches from 'app/configuration/services/Caches';
import FormUtilsFactory from 'app/services/FormUtils.service';
import AgentManager from 'app/modules/agent/AgentManager.service';

export default class ServiceCallFormController {
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


        // TODO: Do we really need this?
        this.$scope.ui = this.IgniteFormUtils.formUI();

        this.formActions = [
            {text: 'Call Service', icon: 'checkmark', click: () => this.confirmAndCall()},
            {text: 'Redeploy Service', icon: 'plus', click: () => this.confirmAndRedeploy(true)}            
        ];
    }

    $onDestroy() {
        
    }

    $onChanges(changes) {
        if (
            'service' in changes && get(this.clonedService, 'id') !== get(this.service, 'id')
        ) {
            this.clonedService = cloneDeep(changes.service.currentValue);
            if (this.$scope.ui && this.$scope.ui.inputForm) {
                this.$scope.ui.inputForm.$setPristine();
                this.$scope.ui.inputForm.$setUntouched();
            }
        }
        
    }

    getValuesToCompare() {
        return [this.service, this.clonedService];
    }

    redeployService(force:boolean) {        
        let serviceName = 'redeployService';
        return this.callServiceForGrid(serviceName,{force});
    }

    
    callServiceForGrid(serviceName:string,params) {
        let args = this.onCall({$event: {serviceName: this.clonedService}});
        let clusterId = args['clusterId'];
        params = Object.assign(args,params);
        this.AgentManager.callClusterService({id: clusterId},serviceName,args).then((data) => {  
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

    reset = (forReal) => forReal ? this.clonedService = cloneDeep(this.service) : void 0;

    confirmAndCall() {
        if (this.$scope.ui.inputForm && this.$scope.ui.inputForm.$invalid)
            return this.IgniteFormUtils.triggerValidation(this.$scope.ui.inputForm, this.$scope);
        return this.IgniteConfirm.confirm('Are you sure you want to call service ' + this.service.name + ' for current grid?')
        .then(() => { this.callServiceForGrid(this.service.id,this.clonedService); } );
    }
    
    confirmAndRedeploy(force:boolean) {        
        if (this.$scope.ui.inputForm && this.$scope.ui.inputForm.$invalid)
            return this.IgniteFormUtils.triggerValidation(this.$scope.ui.inputForm, this.$scope);
        return this.IgniteConfirm.confirm('Are you sure you want to redoploy current service?')
        .then( () => { this.redeployService(force); } );
    }
    
    clearImplementationVersion(storeFactory) {
        delete storeFactory.implementationVersion;
    }
}
