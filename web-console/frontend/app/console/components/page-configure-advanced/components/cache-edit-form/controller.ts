/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import cloneDeep from 'lodash/cloneDeep';
import get from 'lodash/get';
import {tap} from 'rxjs/operators';
import {Menu} from 'app/types';

import LegacyConfirmFactory from 'app/services/Confirm.service';
import Version from 'app/services/Version.service';
import Caches from 'app/configuration/services/Caches';
import FormUtilsFactory from 'app/services/FormUtils.service';

export default class CacheEditFormController {
    modelsMenu: Menu<string>;

    onSave: ng.ICompiledExpression;

    static $inject = ['IgniteConfirm', 'IgniteVersion', '$scope', 'Caches', 'TaskFlows','IgniteFormUtils'];

    constructor(
        private IgniteConfirm: ReturnType<typeof LegacyConfirmFactory>,
        private IgniteVersion: Version,
        private $scope: ng.IScope,
        private Caches: Caches,
        private TaskFlows: TaskFlows,
        private IgniteFormUtils: ReturnType<typeof FormUtilsFactory>
    ) {}

    $onInit() {
        this.available = this.IgniteVersion.available.bind(this.IgniteVersion);

        const rebuildDropdowns = () => {
            this.$scope.affinityFunction = [
                {value: 'Rendezvous', label: 'Rendezvous'},
                {value: 'Custom', label: 'Custom'},
                {value: null, label: 'Default'}
            ];            
        };

        rebuildDropdowns();

        this.subscription = this.IgniteVersion.currentSbj.pipe(
            tap(rebuildDropdowns)            
        )
        .subscribe();        
        
        this.cachesColDefs = [
            {name: 'Name:', cellClass: 'pc-form-grid-col-10'},
            {name: 'ClusterName:', cellClass: 'pc-form-grid-col-10'},
            {name: 'Mode:', cellClass: 'pc-form-grid-col-10'},
            {name: 'Atomicity:', cellClass: 'pc-form-grid-col-20', tip: `
                Atomicity:
                <ul>
                    <li>ATOMIC - in this mode distributed transactions and distributed locking are not supported</li>
                    <li>TRANSACTIONAL - in this mode specified fully ACID-compliant transactional cache behavior</li>
                    <li>TRANSACTIONAL_SNAPSHOT - in this mode specified fully ACID-compliant transactional cache behavior for both key-value API and SQL transactions</li>
                </ul>
            `},
            {name: 'Backups:', cellClass: 'pc-form-grid-col-10', tip: `
                Number of nodes used to back up single partition for partitioned cache
            `}
        ]; 

        // TODO: Do we really need this?
        this.$scope.ui = this.IgniteFormUtils.formUI();

        this.formActions = [
            {text: 'Save', icon: 'checkmark', click: () => this.save()},
            {text: 'Save and Start', icon: 'download', click: () => this.save(true)}
        ];
    }

    $onDestroy() {
        this.subscription.unsubscribe();
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

    save(start) {
        if (this.$scope.ui.inputForm.$invalid)
            return this.IgniteFormUtils.triggerValidation(this.$scope.ui.inputForm, this.$scope);
        this.onSave({$event: {cache: cloneDeep(this.clonedCache), start}});
    }

    reset = (forReal) => forReal ? this.clonedCache = cloneDeep(this.cache) : void 0;

    confirmAndReset() {
        return this.IgniteConfirm.confirm('Are you sure you want to undo all changes for current cache?')
        .then(this.reset);
    }

    clearImplementationVersion(storeFactory) {
        delete storeFactory.implementationVersion;
    }
    
    
    addCache() {
        
    }
    
    removeCache(cache) {
        
    }
    
    changeCache(cache) {
        
    }
}
