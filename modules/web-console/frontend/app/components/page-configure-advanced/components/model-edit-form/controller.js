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
import _ from 'lodash';
import get from 'lodash/get';

import {default as Models} from 'app/services/Models';
import {default as ModalImportModels} from 'app/components/page-configure/components/modal-import-models/service';
import {default as IgniteVersion} from 'app/services/Version.service';
import {Confirm} from 'app/services/Confirm.service';

/** @type {ng.IComponentController} */
export default class ModelEditFormController {
    /** @type {ig.config.model.DomainModel} */
    model;
    /** @type {ng.ICompiledExpression} */
    onSave;

    static $inject = ['ModalImportModels', 'IgniteErrorPopover', 'IgniteLegacyUtils', 'Confirm', 'ConfigChangesGuard', 'IgniteVersion', '$scope', 'Models', 'IgniteFormUtils'];

    /**
     * @param {ModalImportModels} ModalImportModels
     * @param {Confirm} Confirm
     * @param {ng.IScope} $scope
     * @param {Models} Models
     * @param {IgniteVersion} IgniteVersion
     */
    constructor(ModalImportModels, ErrorPopover, LegacyUtils, Confirm, ConfigChangesGuard, IgniteVersion, $scope, Models, IgniteFormUtils) {
        Object.assign(this, {ErrorPopover, LegacyUtils, ConfigChangesGuard, IgniteFormUtils});
        this.ModalImportModels = ModalImportModels;
        this.Confirm = Confirm;
        this.$scope = $scope;
        this.Models = Models;
        this.IgniteVersion = IgniteVersion;
        this.javaBuiltInClassesBase = LegacyUtils.javaBuiltInClasses;
    }
    $onInit() {
        this.available = this.IgniteVersion.available.bind(this.IgniteVersion);

        this.queryFieldTypes = this.LegacyUtils.javaBuiltInClasses.concat('byte[]');
        this.$scope.ui = this.IgniteFormUtils.formUI();

        this.$scope.javaBuiltInClasses = this.LegacyUtils.javaBuiltInClasses;
        this.$scope.supportedJdbcTypes = this.LegacyUtils.mkOptions(this.LegacyUtils.SUPPORTED_JDBC_TYPES);
        this.$scope.supportedJavaTypes = this.LegacyUtils.mkOptions(this.LegacyUtils.javaBuiltInTypes);

        this.formActions = [
            {text: 'Save', icon: 'checkmark', click: () => this.save()},
            {text: 'Save and Download', icon: 'download', click: () => this.save(true)}
        ];
    }

    /**
     * Create list of fields to show in index fields dropdown.
     * @param {string} prefix
     * @param {Array<string>} cur Current queryKeyFields
     */
    fields(prefix, cur) {
        const fields = this.$scope.backupItem
            ? _.map(this.$scope.backupItem.fields, (field) => ({value: field.name, label: field.name}))
            : [];

        if (prefix === 'new')
            return fields;

        _.forEach(_.isArray(cur) ? cur : [cur], (value) => {
            if (!_.find(fields, {value}))
                fields.push({value, label: value + ' (Unknown field)'});
        });

        return fields;
    }

    importModels() {
        return this.ModalImportModels.open();
    }

    /**
     * @param {ig.config.model.DomainModel} item
     */
    checkQueryConfiguration(item) {
        if (item.queryMetadata === 'Configuration' && this.LegacyUtils.domainForQueryConfigured(item)) {
            if (_.isEmpty(item.fields))
                return this.ErrorPopover.show('queryFields', 'Query fields should not be empty', this.$scope.ui, 'query');

            const indexes = item.indexes;

            if (indexes && indexes.length > 0) {
                if (_.find(indexes, (index, idx) => {
                    if (_.isEmpty(index.fields))
                        return !this.ErrorPopover.show('indexes' + idx, 'Index fields are not specified', this.$scope.ui, 'query');

                    if (_.find(index.fields, (field) => !_.find(item.fields, (configuredField) => configuredField.name === field.name)))
                        return !this.ErrorPopover.show('indexes' + idx, 'Index contains not configured fields', this.$scope.ui, 'query');
                }))
                    return false;
            }
        }

        return true;
    }

    /**
     * @param {ig.config.model.DomainModel} item
     */
    checkStoreConfiguration(item) {
        if (this.LegacyUtils.domainForStoreConfigured(item)) {
            if (this.LegacyUtils.isEmptyString(item.databaseSchema))
                return this.ErrorPopover.show('databaseSchemaInput', 'Database schema should not be empty', this.$scope.ui, 'store');

            if (this.LegacyUtils.isEmptyString(item.databaseTable))
                return this.ErrorPopover.show('databaseTableInput', 'Database table should not be empty', this.$scope.ui, 'store');

            if (_.isEmpty(item.keyFields))
                return this.ErrorPopover.show('keyFields', 'Key fields are not specified', this.$scope.ui, 'store');

            if (this.LegacyUtils.isJavaBuiltInClass(item.keyType) && item.keyFields.length !== 1)
                return this.ErrorPopover.show('keyFields', 'Only one field should be specified in case when key type is a Java built-in type', this.$scope.ui, 'store');

            if (_.isEmpty(item.valueFields))
                return this.ErrorPopover.show('valueFields', 'Value fields are not specified', this.$scope.ui, 'store');
        }

        return true;
    }

    /**
     * Check domain model logical consistency.
     * @param {ig.config.model.DomainModel} item
     */
    validate(item) {
        if (!this.checkQueryConfiguration(item))
            return false;

        if (!this.checkStoreConfiguration(item))
            return false;

        if (!this.LegacyUtils.domainForStoreConfigured(item) && !this.LegacyUtils.domainForQueryConfigured(item) && item.queryMetadata === 'Configuration')
            return this.ErrorPopover.show('query-title', 'SQL query domain model should be configured', this.$scope.ui, 'query');

        if (!this.LegacyUtils.domainForStoreConfigured(item) && item.generatePojo)
            return this.ErrorPopover.show('store-title', 'Domain model for cache store should be configured when generation of POJO classes is enabled', this.$scope.ui, 'store');

        return true;
    }

    $onChanges(changes) {
        if (
            'model' in changes && get(this.$scope.backupItem, '_id') !== get(this.model, '_id')
        ) {
            this.$scope.backupItem = cloneDeep(changes.model.currentValue);
            if (this.$scope.ui && this.$scope.ui.inputForm) {
                this.$scope.ui.inputForm.$setPristine();
                this.$scope.ui.inputForm.$setUntouched();
            }
        }
        if ('caches' in changes)
            this.cachesMenu = (changes.caches.currentValue || []).map((c) => ({label: c.name, value: c._id}));
    }

    /**
     * @param {ig.config.model.DomainModel} model
     */
    onQueryFieldsChange(model) {
        this.$scope.backupItem = this.Models.removeInvalidFields(model);
    }

    getValuesToCompare() {
        return [this.model, this.$scope.backupItem].map(this.Models.normalize);
    }

    save(download) {
        if (this.$scope.ui.inputForm.$invalid)
            return this.IgniteFormUtils.triggerValidation(this.$scope.ui.inputForm, this.$scope);

        if (!this.validate(this.$scope.backupItem))
            return;

        this.onSave({$event: {model: cloneDeep(this.$scope.backupItem), download}});
    }

    reset = (forReal) => forReal ? this.$scope.backupItem = cloneDeep(this.model) : void 0;

    confirmAndReset() {
        return this.Confirm.confirm('Are you sure you want to undo all changes for current model?').then(() => true)
        .then(this.reset)
        .catch(() => {});
    }
}
