

import cloneDeep from 'lodash/cloneDeep';
import get from 'lodash/get';
import isEqual from 'lodash/isEqual';
import _ from 'lodash';
import {tap} from 'rxjs/operators';
import {Menu} from 'app/types';

import LegacyUtils from 'app/services/LegacyUtils.service';

import LegacyConfirm from 'app/services/Confirm.service';
import Version from 'app/services/Version.service';
import FormUtils from 'app/services/FormUtils.service';

export default class DatasourceEditFormController {    
    
    onSave: ng.ICompiledExpression;

    static $inject = ['IgniteLegacyUtils', 'IgniteConfirm', 'IgniteVersion', '$scope', 'IgniteFormUtils'];

    constructor(
        private IgniteLegacyUtils: ReturnType<typeof LegacyUtils>,        
        private IgniteConfirm: ReturnType<typeof LegacyConfirm>,
        private IgniteVersion: Version,
        private $scope: ng.IScope,      
        private IgniteFormUtils: ReturnType<typeof FormUtils>
    ) {}

    $onInit() {
        this.available = this.IgniteVersion.available.bind(this.IgniteVersion);

        this.supportedJdbcTypes = this.IgniteLegacyUtils.mkOptions(this.IgniteLegacyUtils.SUPPORTED_JDBC_TYPES);

        this.$scope.ui = this.IgniteFormUtils.formUI();
        this.$scope.ui.loadedPanels = ['attributes', 'transactions'];

        this.formActions = [
            {text: 'Save', icon: 'checkmark', click: () => this.save()},
            {text: 'Save and Publish', icon: 'download', click: () => this.save(true)}
        ];
    }

    $onDestroy() {
        
    }

    $onChanges(changes) {
        if ('cluster' in changes && this.shouldOverwriteValue(this.cluster, this.clonedCluster)) {
            this.clonedCluster = cloneDeep(changes.cluster.currentValue);
            if (this.$scope.ui && this.$scope.ui.inputForm) {
                this.$scope.ui.inputForm.$setPristine();
                this.$scope.ui.inputForm.$setUntouched();
            }
        }
    }

    /**
     * The form should accept incoming cluster value if:
     * 1. It has different id ("new" to real id).
     * 2. Different caches or models (imported from DB).
     * @param a Incoming value.
     * @param b Current value.
     */
    shouldOverwriteValue<T>(a: T, b: T) {
        return get(a, 'id') !== get(b, 'id') ||
            !isEqual(get(a, 'jndiName'), get(b, 'jndiName'));
    }
   
    save(redirect=false) {
        if (this.$scope.ui.inputForm.$invalid){
            return this.IgniteFormUtils.triggerValidation(this.$scope.ui.inputForm, this.$scope);
        }            
        const datasource = cloneDeep(this.clonedCluster)    
        datasource.jdbcProp = {}
        for(let item of datasource.attributes){            
            datasource.jdbcProp[item.name] = item.value
        }
        if(datasource.rebalanceBatchSize){
            datasource.jdbcProp['rebalanceBatchSize'] = datasource.rebalanceBatchSize
            delete datasource.rebalanceBatchSize
        }
        delete datasource.attributes
        this.onSave({$event: {datasource,redirect}});
    }

    reset = () => this.clonedCluster = cloneDeep(this.cluster);

    confirmAndReset() {
        return this.IgniteConfirm
            .confirm('Are you sure you want to undo all changes for current cluster?')
            .then(this.reset);
    }
}
