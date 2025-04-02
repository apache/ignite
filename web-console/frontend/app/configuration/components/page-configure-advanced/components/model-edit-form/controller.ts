

import cloneDeep from 'lodash/cloneDeep';
import _ from 'lodash';
import get from 'lodash/get';

import {default as Models} from '../../../../services/Models';
import {default as ModalImportModels} from '../../../../components/modal-import-models/service';
import {default as IgniteVersion} from 'app/services/Version.service';
import {Confirm} from 'app/services/Confirm.service';
import {DomainModel} from '../../../../types';
import ErrorPopover from 'app/services/ErrorPopover.service';
import JavaTypes from 'app/services/JavaTypes.service';
import SqlTypes from 'app/services/SqlTypes.service';
import LegacyUtilsFactory from 'app/services/LegacyUtils.service';
import ConfigChangesGuard from '../../../../services/ConfigChangesGuard';
import FormUtils from 'app/services/FormUtils.service';

export default class ModelEditFormController {
    model: DomainModel;
    onSave: ng.ICompiledExpression;

    static $inject = ['ModalImportModels', 'IgniteErrorPopover', 'IgniteLegacyUtils', 'JavaTypes', 'SqlTypes', 'Confirm', 'ConfigChangesGuard', 'IgniteVersion', '$scope', 'Models', 'IgniteFormUtils'];

    constructor(
        private ModalImportModels: ModalImportModels,
        private ErrorPopover: ErrorPopover,
        private LegacyUtils: ReturnType<typeof LegacyUtilsFactory>,
        private JavaTypes: JavaTypes,
        private SqlTypes: SqlTypes,
        private Confirm: Confirm,
        private ConfigChangesGuard: ConfigChangesGuard<DomainModel>,
        private IgniteVersion: IgniteVersion,
        private $scope: ng.IScope,
        private Models: Models,
        private IgniteFormUtils: ReturnType<typeof FormUtils>
    ) {}
    

    $onInit() {
        this.available = this.IgniteVersion.available.bind(this.IgniteVersion);
        this.$scope.ui = this.IgniteFormUtils.formUI();
        this.queryFieldTypes = this.JavaTypes.javaBuiltInClasses();
        this.javaBuiltInClassesBase = this.JavaTypes.javaBuiltInBaseClasses();

        this.$scope.supportedJdbcTypes = this.SqlTypes.mkJdbcTypeOptions();
        this.$scope.supportedJavaTypes = this.JavaTypes.mkJavaBuiltInTypesOptions();

        this.formActions = [
            {text: 'Save', icon: 'checkmark', click: () => this.save(false)},
            {text: 'Save and Download', icon: 'download', click: () => this.save(true)}
        ];
    }

    /**
     * Create list of fields to show in index fields dropdown.
     * @param cur Current queryKeyFields
     */
    fields(prefix: string, cur: string[]) {
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

    importModelsFromTemplate() {
        return this.ModalImportModels.openTemplate();
    }

    checkQueryConfiguration(item: DomainModel) {
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

    checkStoreConfiguration(item: DomainModel) {
        if (this.LegacyUtils.domainForStoreConfigured(item)) {
            if (this.LegacyUtils.isEmptyString(item.databaseSchema))
                return this.ErrorPopover.show('databaseSchemaInput', 'Database schema should not be empty', this.$scope.ui, 'store');

            if (this.LegacyUtils.isEmptyString(item.databaseTable))
                return this.ErrorPopover.show('databaseTableInput', 'Database table should not be empty', this.$scope.ui, 'store');

            if (_.isEmpty(item.keyFields))
                return this.ErrorPopover.show('keyFields', 'Key fields are not specified', this.$scope.ui, 'store');

            if (this.JavaTypes.isJavaBuiltInClass(item.keyType) && item.keyFields.length !== 1)
                return this.ErrorPopover.show('keyFields', 'Only one field should be specified in case when key type is a Java built-in type', this.$scope.ui, 'store');

            if (_.isEmpty(item.valueFields))
                return this.ErrorPopover.show('valueFields', 'Value fields are not specified', this.$scope.ui, 'store');
        }

        return true;
    }

    /**
     * Check domain model logical consistency.
     */
    validate(item: DomainModel) {
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
            'model' in changes && get(this.$scope.backupItem, 'id') !== get(this.model, 'id')
        ) {
            this.$scope.backupItem = cloneDeep(changes.model.currentValue);
            if (this.$scope.ui && this.$scope.ui.inputForm) {
                this.$scope.ui.inputForm.$setPristine();
                this.$scope.ui.inputForm.$setUntouched();
            }
        }
        if ('caches' in changes)
            this.cachesMenu = (changes.caches.currentValue || []).map((c) => ({label: c.name, value: c.id}));
    }

    onQueryFieldsChange(model: DomainModel) {
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

    reset = (forReal: boolean) => forReal ? this.$scope.backupItem = cloneDeep(this.model) : void 0;

    confirmAndReset() {
        return this.Confirm.confirm('Are you sure you want to undo all changes for current model?').then(() => true)
        .then(this.reset)
        .catch(() => {});
    }
}
