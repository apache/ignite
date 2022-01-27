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
import isEqual from 'lodash/isEqual';
import _ from 'lodash';
import {tap} from 'rxjs/operators';
import {ShortCache} from '../../../types';
import {Menu} from 'app/types';

import LegacyUtils from 'app/services/LegacyUtils.service';

import LegacyConfirm from 'app/services/Confirm.service';
import Version from 'app/services/Version.service';
import FormUtils from 'app/services/FormUtils.service';

export default class ClusterEditFormController {    
    
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
        this.$scope.ui.loadedPanels = ['sql-connector'];

        this.formActions = [
            {text: 'Save', icon: 'checkmark', click: () => this.save()},
            {text: 'Save and Download', icon: 'download', click: () => this.save(true)}
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
            !isEqual(get(a, 'caches'), get(b, 'caches')) ||
            !isEqual(get(a, 'models'), get(b, 'models'));
    }
   
    save(download) {
        if (this.$scope.ui.inputForm.$invalid)
            return this.IgniteFormUtils.triggerValidation(this.$scope.ui.inputForm, this.$scope);

        this.onSave({$event: {cluster: cloneDeep(this.clonedCluster), download}});
    }

    reset = () => this.clonedCluster = cloneDeep(this.cluster);

    confirmAndReset() {
        return this.IgniteConfirm
            .confirm('Are you sure you want to undo all changes for current cluster?')
            .then(this.reset);
    }
}
