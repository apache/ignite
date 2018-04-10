/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import cloneDeep from 'lodash/cloneDeep';
import get from 'lodash/get';

export default class IgfsEditFormController {
    static $inject = ['IgniteConfirm', 'IgniteVersion', '$scope', 'IGFSs', 'IgniteFormUtils'];
    constructor( IgniteConfirm, IgniteVersion, $scope, IGFSs, IgniteFormUtils) {
        Object.assign(this, { IgniteConfirm, IgniteVersion, $scope, IGFSs, IgniteFormUtils});
    }
    $onInit() {
        this.available = this.IgniteVersion.available.bind(this.IgniteVersion);

        this.$scope.ui = this.IgniteFormUtils.formUI();
        this.$scope.ui.loadedPanels = ['general', 'secondaryFileSystem', 'misc'];
    }

    $onChanges(changes) {
        if (
            'igfs' in changes && get(this.$scope.backupItem, '_id') !== get(this.igfs, '_id')
        ) {
            this.$scope.backupItem = cloneDeep(changes.igfs.currentValue);
            if (this.$scope.ui && this.$scope.ui.inputForm) {
                this.$scope.ui.inputForm.$setPristine();
                this.$scope.ui.inputForm.$setUntouched();
            }
        }
    }
    getValuesToCompare() {
        return [this.igfs, this.$scope.backupItem].map(this.IGFSs.normalize);
    }
    save() {
        if (this.$scope.ui.inputForm.$invalid)
            return this.IgniteFormUtils.triggerValidation(this.$scope.ui.inputForm, this.$scope);
        this.onSave({$event: cloneDeep(this.$scope.backupItem)});
    }
    reset = (forReal) => forReal ? this.$scope.backupItem = cloneDeep(this.igfs) : void 0;
    confirmAndReset() {
        return this.IgniteConfirm.confirm('Are you sure you want to undo all changes for current IGFS?')
        .then(this.reset);
    }
}
