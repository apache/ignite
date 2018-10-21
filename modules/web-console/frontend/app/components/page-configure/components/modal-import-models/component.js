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

import templateUrl from './template.tpl.pug';
import './style.scss';
import _ from 'lodash';
import naturalCompare from 'natural-compare-lite';
import find from 'lodash/fp/find';
import get from 'lodash/fp/get';
import {Observable} from 'rxjs/Observable';
import ObjectID from 'bson-objectid';
import {uniqueName} from 'app/utils/uniqueName';
import {defaultNames} from '../../defaultNames';

// eslint-disable-next-line
import {UIRouter} from '@uirouter/angularjs'
import {default as IgniteConfirmBatch} from 'app/services/ConfirmBatch.service';
import {default as ConfigSelectors} from 'app/components/page-configure/store/selectors';
import {default as ConfigEffects} from 'app/components/page-configure/store/effects';
import {default as ConfigureState} from 'app/components/page-configure/services/ConfigureState';
// eslint-disable-next-line
import {default as AgentManager} from 'app/modules/agent/AgentModal.service'
import {default as SqlTypes} from 'app/services/SqlTypes.service';
import {default as JavaTypes} from 'app/services/JavaTypes.service';
// eslint-disable-next-line
import {default as ActivitiesData} from 'app/core/activities/Activities.data';

function _mapCaches(caches = []) {
    return caches.map((cache) => {
        return {label: cache.name, value: cache._id, cache};
    });
}

const INFO_CONNECT_TO_DB = 'Configure connection to database';
const INFO_SELECT_SCHEMAS = 'Select schemas to load tables from';
const INFO_SELECT_TABLES = 'Select tables to import as domain model';
const INFO_SELECT_OPTIONS = 'Select import domain model options';
const LOADING_JDBC_DRIVERS = {text: 'Loading JDBC drivers...'};
const LOADING_SCHEMAS = {text: 'Loading schemas...'};
const LOADING_TABLES = {text: 'Loading tables...'};
const SAVING_DOMAINS = {text: 'Saving domain model...'};

const IMPORT_DM_NEW_CACHE = 1;
const IMPORT_DM_ASSOCIATE_CACHE = 2;

const DFLT_PARTITIONED_CACHE = {
    label: 'PARTITIONED',
    value: -1,
    cache: {
        name: 'PARTITIONED',
        cacheMode: 'PARTITIONED',
        atomicityMode: 'ATOMIC',
        readThrough: true,
        writeThrough: true
    }
};

const DFLT_REPLICATED_CACHE = {
    label: 'REPLICATED',
    value: -2,
    cache: {
        name: 'REPLICATED',
        cacheMode: 'REPLICATED',
        atomicityMode: 'ATOMIC',
        readThrough: true,
        writeThrough: true
    }
};

const CACHE_TEMPLATES = [DFLT_PARTITIONED_CACHE, DFLT_REPLICATED_CACHE];

export class ModalImportModels {
    /**
     * Cluster ID to import models into
     * @type {string}
     */
    clusterID;

    /** @type {ng.ICompiledExpression} */
    onHide;

    static $inject = ['$uiRouter', 'ConfigSelectors', 'ConfigEffects', 'ConfigureState', '$http', 'IgniteConfirm', 'IgniteConfirmBatch', 'IgniteFocus', 'SqlTypes', 'JavaTypes', 'IgniteMessages', '$scope', '$rootScope', 'AgentManager', 'IgniteActivitiesData', 'IgniteLoading', 'IgniteFormUtils', 'IgniteLegacyUtils'];

    /**
     * @param {UIRouter} $uiRouter
     * @param {ConfigSelectors} ConfigSelectors
     * @param {ConfigEffects} ConfigEffects
     * @param {ConfigureState} ConfigureState
     * @param {ng.IHttpService} $http
     * @param {IgniteConfirmBatch} ConfirmBatch
     * @param {SqlTypes} SqlTypes
     * @param {JavaTypes} JavaTypes
     * @param {ng.IScope} $scope
     * @param {ng.IRootScopeService} $root
     * @param {AgentManager} agentMgr
     * @param {ActivitiesData} ActivitiesData
     */
    constructor($uiRouter, ConfigSelectors, ConfigEffects, ConfigureState, $http, Confirm, ConfirmBatch, Focus, SqlTypes, JavaTypes, Messages, $scope, $root, agentMgr, ActivitiesData, Loading, FormUtils, LegacyUtils) {
        this.$uiRouter = $uiRouter;
        this.ConfirmBatch = ConfirmBatch;
        this.$http = $http;
        this.ConfigSelectors = ConfigSelectors;
        this.ConfigEffects = ConfigEffects;
        this.ConfigureState = ConfigureState;
        this.$root = $root;
        this.$scope = $scope;
        this.agentMgr = agentMgr;
        this.JavaTypes = JavaTypes;
        this.SqlTypes = SqlTypes;
        this.ActivitiesData = ActivitiesData;
        Object.assign(this, {Confirm, Focus, Messages, Loading, FormUtils, LegacyUtils});
    }

    loadData() {
        return Observable.of(this.clusterID)
            .switchMap((id = 'new') => {
                return this.ConfigureState.state$.let(this.ConfigSelectors.selectClusterToEdit(id, defaultNames.importedCluster));
            })
        .switchMap((cluster) => {
            return (!(cluster.caches || []).length && !(cluster.models || []).length)
                ? Observable.of({
                    cluster,
                    caches: [],
                    models: []
                })
                : Observable.fromPromise(Promise.all([
                    this.ConfigEffects.etp('LOAD_SHORT_CACHES', {ids: cluster.caches || [], clusterID: cluster._id}),
                    this.ConfigEffects.etp('LOAD_SHORT_MODELS', {ids: cluster.models || [], clusterID: cluster._id})
                ]))
                .switchMap(() => {
                    return Observable.combineLatest(
                        this.ConfigureState.state$.let(this.ConfigSelectors.selectShortCachesValue()),
                        this.ConfigureState.state$.let(this.ConfigSelectors.selectShortModelsValue()),
                        (caches, models) => ({
                            cluster,
                            caches,
                            models
                        })
                    ).take(1);
                });
        })
        .take(1);
    }

    saveBatch(batch) {
        if (!batch.length)
            return;

        this.$scope.importDomain.loadingOptions = SAVING_DOMAINS;
        this.Loading.start('importDomainFromDb');

        this.ConfigureState.dispatchAction({
            type: 'ADVANCED_SAVE_COMPLETE_CONFIGURATION',
            changedItems: this.batchActionsToRequestBody(batch),
            prevActions: []
        });

        this.saveSubscription = Observable.race(
            this.ConfigureState.actions$.filter((a) => a.type === 'ADVANCED_SAVE_COMPLETE_CONFIGURATION_OK')
                .do(() => this.onHide()),
            this.ConfigureState.actions$.filter((a) => a.type === 'ADVANCED_SAVE_COMPLETE_CONFIGURATION_ERR')
        )
        .take(1)
        .do(() => {
            this.Loading.finish('importDomainFromDb');
        })
        .subscribe();
    }

    batchActionsToRequestBody(batch) {
        const result = batch.reduce((req, action) => {
            return {
                ...req,
                cluster: {
                    ...req.cluster,
                    models: [...req.cluster.models, action.newDomainModel._id],
                    caches: [...req.cluster.caches, ...action.newDomainModel.caches]
                },
                models: [...req.models, action.newDomainModel],
                caches: action.newCache
                    ? [...req.caches, action.newCache]
                    : action.cacheStoreChanges
                        ? [...req.caches, {
                            ...this.loadedCaches[action.cacheStoreChanges[0].cacheId],
                            ...action.cacheStoreChanges[0].change
                        }]
                        : req.caches
            };
        }, {cluster: this.cluster, models: [], caches: [], igfss: []});
        result.cluster.models = [...new Set(result.cluster.models)];
        result.cluster.caches = [...new Set(result.cluster.caches)];
        return result;
    }

    onTableSelectionChange(selected) {
        this.$scope.$applyAsync(() => {
            this.$scope.importDomain.tablesToUse = selected;
            this.selectedTablesIDs = selected.map((t) => t.id);
        });
    }

    onSchemaSelectionChange(selected) {
        this.$scope.$applyAsync(() => {
            this.$scope.importDomain.schemasToUse = selected;
            this.selectedSchemasIDs = selected.map((i) => i.name);
        });
    }

    onVisibleRowsChange(rows) {
        return this.visibleTables = rows.map((r) => r.entity);
    }

    onCacheSelect(cacheID) {
        if (cacheID < 0)
            return;

        if (this.loadedCaches[cacheID])
            return;

        return this.onCacheSelectSubcription = Observable.merge(
            Observable.timer(0, 1).take(1)
                .do(() => this.ConfigureState.dispatchAction({type: 'LOAD_CACHE', cacheID})),
            Observable.race(
                this.ConfigureState.actions$
                    .filter((a) => a.type === 'LOAD_CACHE_OK' && a.cache._id === cacheID).pluck('cache')
                    .do((cache) => {
                        this.loadedCaches[cacheID] = cache;
                    }),
                this.ConfigureState.actions$
                    .filter((a) => a.type === 'LOAD_CACHE_ERR' && a.action.cacheID === cacheID)
            ).take(1)
        )
        .subscribe();
    }

    $onDestroy() {
        this.subscription.unsubscribe();
        if (this.onCacheSelectSubcription) this.onCacheSelectSubcription.unsubscribe();
        if (this.saveSubscription) this.saveSubscription.unsubscribe();
    }

    $onInit() {
        // Restores old behavior
        const {$http, Confirm, ConfirmBatch, Focus, SqlTypes, JavaTypes, Messages, $scope, $root, agentMgr, ActivitiesData, Loading, FormUtils, LegacyUtils} = this;

        /**
         * Convert some name to valid java package name.
         *
         * @param name to convert.
         * @returns {string} Valid java package name.
         */
        const _toJavaPackage = (name) => {
            return name ? name.replace(/[^A-Za-z_0-9/.]+/g, '_') : 'org';
        };

        const importDomainModal = {
            hide: () => {
                agentMgr.stopWatch();
                this.onHide();
            }
        };

        const _makeDefaultPackageName = (user) => user
            ? _toJavaPackage(`${user.email.replace('@', '.').split('.').reverse().join('.')}.model`)
            : void 0;

        this.$scope.ui = {
            generatePojo: true,
            builtinKeys: true,
            generateKeyFields: true,
            usePrimitives: true,
            generateTypeAliases: true,
            generateFieldAliases: true,
            packageNameUserInput: _makeDefaultPackageName($root.user)
        };
        this.$scope.$hide = importDomainModal.hide;

        this.$scope.importCommon = {};

        this.subscription = this.loadData().do((data) => {
            this.$scope.caches = _mapCaches(data.caches);
            this.$scope.domains = data.models;
            this.caches = data.caches;
            this.cluster = data.cluster;

            if (!_.isEmpty(this.$scope.caches)) {
                this.$scope.importActions.push({
                    label: 'Associate with existing cache',
                    shortLabel: 'Associate',
                    value: IMPORT_DM_ASSOCIATE_CACHE
                });
            }
            this.$scope.$watch('importCommon.action', this._fillCommonCachesOrTemplates(this.$scope.importCommon), true);
            this.$scope.importCommon.action = IMPORT_DM_NEW_CACHE;
        }).subscribe();

        // New
        this.loadedCaches = {
            ...CACHE_TEMPLATES.reduce((a, c) => ({...a, [c.value]: c.cache}), {})
        };

        this.actions = [
            {value: 'connect', label: this.$root.IgniteDemoMode ? 'Description' : 'Connection'},
            {value: 'schemas', label: 'Schemas'},
            {value: 'tables', label: 'Tables'},
            {value: 'options', label: 'Options'}
        ];

        // Legacy
        $scope.ui.invalidKeyFieldsTooltip = 'Found key types without configured key fields<br/>' +
            'It may be a result of import tables from database without primary keys<br/>' +
            'Key field for such key types should be configured manually';

        $scope.indexType = LegacyUtils.mkOptions(['SORTED', 'FULLTEXT', 'GEOSPATIAL']);

        $scope.importActions = [{
            label: 'Create new cache by template',
            shortLabel: 'Create',
            value: IMPORT_DM_NEW_CACHE
        }];


        const _dbPresets = [
            {
                db: 'Oracle',
                jdbcDriverClass: 'oracle.jdbc.OracleDriver',
                jdbcUrl: 'jdbc:oracle:thin:@[host]:[port]:[database]',
                user: 'system'
            },
            {
                db: 'DB2',
                jdbcDriverClass: 'com.ibm.db2.jcc.DB2Driver',
                jdbcUrl: 'jdbc:db2://[host]:[port]/[database]',
                user: 'db2admin'
            },
            {
                db: 'SQLServer',
                jdbcDriverClass: 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
                jdbcUrl: 'jdbc:sqlserver://[host]:[port][;databaseName=database]'
            },
            {
                db: 'PostgreSQL',
                jdbcDriverClass: 'org.postgresql.Driver',
                jdbcUrl: 'jdbc:postgresql://[host]:[port]/[database]',
                user: 'sa'
            },
            {
                db: 'MySQL',
                jdbcDriverClass: 'com.mysql.jdbc.Driver',
                jdbcUrl: 'jdbc:mysql://[host]:[port]/[database]',
                user: 'root'
            },
            {
                db: 'MySQL',
                jdbcDriverClass: 'org.mariadb.jdbc.Driver',
                jdbcUrl: 'jdbc:mariadb://[host]:[port]/[database]',
                user: 'root'
            },
            {
                db: 'H2',
                jdbcDriverClass: 'org.h2.Driver',
                jdbcUrl: 'jdbc:h2:tcp://[host]/[database]',
                user: 'sa'
            }
        ];

        $scope.selectedPreset = {
            db: 'Generic',
            jdbcDriverJar: '',
            jdbcDriverClass: '',
            jdbcUrl: 'jdbc:[database]',
            user: 'sa',
            password: '',
            tablesOnly: true
        };

        $scope.demoConnection = {
            db: 'H2',
            jdbcDriverClass: 'org.h2.Driver',
            jdbcUrl: 'jdbc:h2:mem:demo-db',
            user: 'sa',
            password: '',
            tablesOnly: true
        };

        function _loadPresets() {
            try {
                const restoredPresets = JSON.parse(localStorage.dbPresets);

                _.forEach(restoredPresets, (restoredPreset) => {
                    const preset = _.find(_dbPresets, {jdbcDriverClass: restoredPreset.jdbcDriverClass});

                    if (preset) {
                        preset.jdbcUrl = restoredPreset.jdbcUrl;
                        preset.user = restoredPreset.user;
                    }
                });
            }
            catch (ignore) {
                // No-op.
            }
        }

        _loadPresets();

        function _savePreset(preset) {
            try {
                const oldPreset = _.find(_dbPresets, {jdbcDriverClass: preset.jdbcDriverClass});

                if (oldPreset)
                    _.assign(oldPreset, preset);
                else
                    _dbPresets.push(preset);

                localStorage.dbPresets = JSON.stringify(_dbPresets);
            }
            catch (err) {
                Messages.showError(err);
            }
        }

        function _findPreset(selectedJdbcJar) {
            let result = _.find(_dbPresets, function(preset) {
                return preset.jdbcDriverClass === selectedJdbcJar.jdbcDriverClass;
            });

            if (!result)
                result = {db: 'Generic', jdbcUrl: 'jdbc:[database]', user: 'admin'};

            result.jdbcDriverJar = selectedJdbcJar.jdbcDriverJar;
            result.jdbcDriverClass = selectedJdbcJar.jdbcDriverClass;

            return result;
        }

        function isValidJavaIdentifier(s) {
            return JavaTypes.validIdentifier(s) && !JavaTypes.isKeyword(s) && JavaTypes.nonBuiltInClass(s) &&
                SqlTypes.validIdentifier(s) && !SqlTypes.isKeyword(s);
        }

        function toJavaIdentifier(name) {
            if (_.isEmpty(name))
                return 'DB';

            const len = name.length;

            let ident = '';

            let capitalizeNext = true;

            for (let i = 0; i < len; i++) {
                const ch = name.charAt(i);

                if (ch === ' ' || ch === '_')
                    capitalizeNext = true;
                else if (ch === '-') {
                    ident += '_';
                    capitalizeNext = true;
                }
                else if (capitalizeNext) {
                    ident += ch.toLocaleUpperCase();

                    capitalizeNext = false;
                }
                else
                    ident += ch.toLocaleLowerCase();
            }

            return ident;
        }

        function toJavaClassName(name) {
            const clazzName = toJavaIdentifier(name);

            if (isValidJavaIdentifier(clazzName))
                return clazzName;

            return 'Class' + clazzName;
        }

        function toJavaFieldName(dbName) {
            const javaName = toJavaIdentifier(dbName);

            const fieldName = javaName.charAt(0).toLocaleLowerCase() + javaName.slice(1);

            if (isValidJavaIdentifier(fieldName))
                return fieldName;

            return 'field' + javaName;
        }

        /**
         * Load list of database schemas.
         */
        const _loadSchemas = () => {
            agentMgr.awaitAgent()
                .then(function() {
                    $scope.importDomain.loadingOptions = LOADING_SCHEMAS;
                    Loading.start('importDomainFromDb');

                    if ($root.IgniteDemoMode)
                        return agentMgr.schemas($scope.demoConnection);

                    const preset = $scope.selectedPreset;

                    _savePreset(preset);

                    return agentMgr.schemas(preset);
                })
                .then((schemaInfo) => {
                    $scope.importDomain.action = 'schemas';
                    $scope.importDomain.info = INFO_SELECT_SCHEMAS;
                    $scope.importDomain.catalog = toJavaIdentifier(schemaInfo.catalog);
                    $scope.importDomain.schemas = _.map(schemaInfo.schemas, (schema) => ({name: schema}));
                    $scope.importDomain.schemasToUse = $scope.importDomain.schemas;
                    this.selectedSchemasIDs = $scope.importDomain.schemas.map((s) => s.name);

                    if ($scope.importDomain.schemas.length === 0)
                        $scope.importDomainNext();
                })
                .catch(Messages.showError)
                .then(() => Loading.finish('importDomainFromDb'));
        };


        this._importCachesOrTemplates = [];

        $scope.tableActionView = (tbl) => {
            const cacheName = get('label')(find({value: tbl.cacheOrTemplate}));

            if (tbl.action === IMPORT_DM_NEW_CACHE)
                return 'Create ' + tbl.generatedCacheName + ' (' + cacheName + ')';

            return 'Associate with ' + cacheName;
        };

        /**
         * Load list of database tables.
         */
        const _loadTables = () => {
            agentMgr.awaitAgent()
                .then(() => {
                    $scope.importDomain.loadingOptions = LOADING_TABLES;
                    Loading.start('importDomainFromDb');

                    $scope.importDomain.allTablesSelected = false;
                    this.selectedTables = [];

                    const preset = $scope.importDomain.demo ? $scope.demoConnection : $scope.selectedPreset;

                    preset.schemas = $scope.importDomain.schemasToUse.map((s) => s.name);

                    return agentMgr.tables(preset);
                })
                .then((tables) => {
                    this._importCachesOrTemplates = CACHE_TEMPLATES.concat($scope.caches);

                    this._fillCommonCachesOrTemplates($scope.importCommon)($scope.importCommon.action);

                    _.forEach(tables, (tbl, idx) => {
                        tbl.id = idx;
                        tbl.action = IMPORT_DM_NEW_CACHE;
                        // tbl.generatedCacheName = toJavaClassName(tbl.table) + 'Cache';
                        tbl.generatedCacheName = uniqueName(toJavaClassName(tbl.table) + 'Cache', this.caches);
                        tbl.cacheOrTemplate = DFLT_PARTITIONED_CACHE.value;
                        tbl.label = tbl.schema + '.' + tbl.table;
                        tbl.edit = false;
                    });

                    $scope.importDomain.action = 'tables';
                    $scope.importDomain.tables = tables;
                    const tablesToUse = tables.filter((t) => LegacyUtils.isDefined(_.find(t.columns, (col) => col.key)));
                    this.selectedTablesIDs = tablesToUse.map((t) => t.id);
                    this.$scope.importDomain.tablesToUse = tablesToUse;

                    $scope.importDomain.info = INFO_SELECT_TABLES;
                })
                .catch(Messages.showError)
                .then(() => Loading.finish('importDomainFromDb'));
        };

        $scope.applyDefaults = () => {
            _.forEach(this.visibleTables, (table) => {
                table.edit = false;
                table.action = $scope.importCommon.action;
                table.cacheOrTemplate = $scope.importCommon.cacheOrTemplate;
            });
        };

        $scope._curDbTable = null;

        $scope.startEditDbTableCache = (tbl) => {
            if ($scope._curDbTable) {
                $scope._curDbTable.edit = false;

                if ($scope._curDbTable.actionWatch) {
                    $scope._curDbTable.actionWatch();
                    $scope._curDbTable.actionWatch = null;
                }
            }

            $scope._curDbTable = tbl;

            const _fillFn = this._fillCommonCachesOrTemplates($scope._curDbTable);

            _fillFn($scope._curDbTable.action);

            $scope._curDbTable.actionWatch = $scope.$watch('_curDbTable.action', _fillFn, true);

            $scope._curDbTable.edit = true;
        };

        /**
         * Show page with import domain models options.
         */
        function _selectOptions() {
            $scope.importDomain.action = 'options';
            $scope.importDomain.button = 'Save';
            $scope.importDomain.info = INFO_SELECT_OPTIONS;

            Focus.move('domainPackageName');
        }

        const _saveDomainModel = (optionsForm) => {
            if (optionsForm.$invalid)
                return this.FormUtils.triggerValidation(optionsForm, this.$scope);

            const generatePojo = $scope.ui.generatePojo;
            const packageName = $scope.ui.packageName;

            const batch = [];
            const checkedCaches = [];

            let containKey = true;
            let containDup = false;

            function dbField(name, jdbcType, nullable, unsigned) {
                const javaTypes = (unsigned && jdbcType.unsigned) ? jdbcType.unsigned : jdbcType.signed;
                const javaFieldType = (!nullable && javaTypes.primitiveType && $scope.ui.usePrimitives) ? javaTypes.primitiveType : javaTypes.javaType;

                return {
                    databaseFieldName: name,
                    databaseFieldType: jdbcType.dbName,
                    javaType: javaTypes.javaType,
                    javaFieldName: toJavaFieldName(name),
                    javaFieldType
                };
            }

            _.forEach($scope.importDomain.tablesToUse, (table, curIx, tablesToUse) => {
                const qryFields = [];
                const indexes = [];
                const keyFields = [];
                const valFields = [];
                const aliases = [];

                const tableName = table.table;
                let typeName = toJavaClassName(tableName);

                if (_.find($scope.importDomain.tablesToUse,
                        (tbl, ix) => ix !== curIx && tableName === tbl.table)) {
                    typeName = typeName + '_' + toJavaClassName(table.schema);

                    containDup = true;
                }

                let valType = tableName;
                let typeAlias;

                if (generatePojo) {
                    if ($scope.ui.generateTypeAliases && tableName.toLowerCase() !== typeName.toLowerCase())
                        typeAlias = tableName;

                    valType = _toJavaPackage(packageName) + '.' + typeName;
                }

                let _containKey = false;

                _.forEach(table.columns, function(col) {
                    const fld = dbField(col.name, SqlTypes.findJdbcType(col.type), col.nullable, col.unsigned);

                    qryFields.push({name: fld.javaFieldName, className: fld.javaType});

                    const dbName = fld.databaseFieldName;

                    if (generatePojo && $scope.ui.generateFieldAliases &&
                        SqlTypes.validIdentifier(dbName) && !SqlTypes.isKeyword(dbName) &&
                        !_.find(aliases, {field: fld.javaFieldName}) &&
                        fld.javaFieldName.toUpperCase() !== dbName.toUpperCase())
                        aliases.push({field: fld.javaFieldName, alias: dbName});

                    if (col.key) {
                        keyFields.push(fld);

                        _containKey = true;
                    }
                    else
                        valFields.push(fld);
                });

                containKey &= _containKey;
                if (table.indexes) {
                    _.forEach(table.indexes, (idx) => {
                        const idxFields = _.map(idx.fields, (idxFld) => ({
                            name: toJavaFieldName(idxFld.name),
                            direction: idxFld.sortOrder
                        }));

                        indexes.push({
                            name: idx.name,
                            indexType: 'SORTED',
                            fields: idxFields
                        });
                    });
                }

                const domainFound = _.find($scope.domains, (domain) => domain.valueType === valType);

                const batchAction = {
                    confirm: false,
                    skip: false,
                    table,
                    newDomainModel: {
                        _id: ObjectID.generate(),
                        caches: [],
                        generatePojo
                    }
                };

                if (LegacyUtils.isDefined(domainFound)) {
                    batchAction.newDomainModel._id = domainFound._id;
                    // Don't touch original caches value
                    delete batchAction.newDomainModel.caches;
                    batchAction.confirm = true;
                }

                Object.assign(batchAction.newDomainModel, {
                    tableName: typeAlias,
                    keyType: valType + 'Key',
                    valueType: valType,
                    queryMetadata: 'Configuration',
                    databaseSchema: table.schema,
                    databaseTable: tableName,
                    fields: qryFields,
                    queryKeyFields: _.map(keyFields, (field) => field.javaFieldName),
                    indexes,
                    keyFields,
                    aliases,
                    valueFields: _.isEmpty(valFields) ? keyFields.slice() : valFields
                });

                // Use Java built-in type for key.
                if ($scope.ui.builtinKeys && batchAction.newDomainModel.keyFields.length === 1) {
                    const newDomain = batchAction.newDomainModel;
                    const keyField = newDomain.keyFields[0];

                    newDomain.keyType = keyField.javaType;
                    newDomain.keyFieldName = keyField.javaFieldName;

                    if (!$scope.ui.generateKeyFields) {
                        // Exclude key column from query fields.
                        newDomain.fields = _.filter(newDomain.fields, (field) => field.name !== keyField.javaFieldName);

                        newDomain.queryKeyFields = [];
                    }

                    // Exclude key column from indexes.
                    _.forEach(newDomain.indexes, (index) => {
                        index.fields = _.filter(index.fields, (field) => field.name !== keyField.javaFieldName);
                    });

                    newDomain.indexes = _.filter(newDomain.indexes, (index) => !_.isEmpty(index.fields));
                }

                // Prepare caches for generation.
                if (table.action === IMPORT_DM_NEW_CACHE) {
                    const newCache = _.cloneDeep(this.loadedCaches[table.cacheOrTemplate]);

                    batchAction.newCache = newCache;

                    // const siblingCaches = batch.filter((a) => a.newCache).map((a) => a.newCache);
                    const siblingCaches = [];
                    newCache._id = ObjectID.generate();
                    newCache.name = uniqueName(typeName + 'Cache', this.caches.concat(siblingCaches));
                    newCache.domains = [batchAction.newDomainModel._id];
                    batchAction.newDomainModel.caches = [newCache._id];

                    // POJO store factory is not defined in template.
                    if (!newCache.cacheStoreFactory || newCache.cacheStoreFactory.kind !== 'CacheJdbcPojoStoreFactory') {
                        const dialect = $scope.importDomain.demo ? 'H2' : $scope.selectedPreset.db;

                        const catalog = $scope.importDomain.catalog;

                        newCache.cacheStoreFactory = {
                            kind: 'CacheJdbcPojoStoreFactory',
                            CacheJdbcPojoStoreFactory: {
                                dataSourceBean: 'ds' + dialect + '_' + catalog,
                                dialect
                            },
                            CacheJdbcBlobStoreFactory: { connectVia: 'DataSource' }
                        };
                    }

                    if (!newCache.readThrough && !newCache.writeThrough) {
                        newCache.readThrough = true;
                        newCache.writeThrough = true;
                    }
                }
                else {
                    const newDomain = batchAction.newDomainModel;
                    const cacheId = table.cacheOrTemplate;

                    batchAction.newDomainModel.caches = [cacheId];

                    if (!_.includes(checkedCaches, cacheId)) {
                        const cache = _.find($scope.caches, {value: cacheId}).cache;

                        // TODO: move elsewhere, make sure it still works
                        const change = LegacyUtils.autoCacheStoreConfiguration(cache, [newDomain]);

                        if (change)
                            batchAction.cacheStoreChanges = [{cacheId, change}];

                        checkedCaches.push(cacheId);
                    }
                }

                batch.push(batchAction);
            });

            /**
             * Generate message to show on confirm dialog.
             *
             * @param meta Object to confirm.
             * @returns {string} Generated message.
             */
            function overwriteMessage(meta) {
                return `
                    Domain model with name &quot;${meta.newDomainModel.databaseTable}&quot; already exists.
                    Are you sure you want to overwrite it?
                `;
            }

            const itemsToConfirm = _.filter(batch, (item) => item.confirm);

            const checkOverwrite = () => {
                if (itemsToConfirm.length > 0) {
                    return ConfirmBatch.confirm(overwriteMessage, itemsToConfirm)
                        .then(() => this.saveBatch(_.filter(batch, (item) => !item.skip)))
                        .catch(() => Messages.showError('Importing of domain models interrupted by user.'));
                }
                return this.saveBatch(batch);
            };

            const checkDuplicate = () => {
                if (containDup) {
                    Confirm.confirm('Some tables have the same name.<br/>' +
                        'Name of types for that tables will contain schema name too.')
                        .then(() => checkOverwrite());
                }
                else
                    checkOverwrite();
            };

            if (containKey)
                checkDuplicate();
            else {
                Confirm.confirm('Some tables have no primary key.<br/>' +
                    'You will need to configure key type and key fields for such tables after import complete.')
                    .then(() => checkDuplicate());
            }
        };


        $scope.importDomainNext = (form) => {
            if (!$scope.importDomainNextAvailable())
                return;

            const act = $scope.importDomain.action;

            if (act === 'drivers' && $scope.importDomain.jdbcDriversNotFound)
                importDomainModal.hide();
            else if (act === 'connect')
                _loadSchemas();
            else if (act === 'schemas')
                _loadTables();
            else if (act === 'tables')
                _selectOptions();
            else if (act === 'options')
                _saveDomainModel(form);
        };

        $scope.nextTooltipText = function() {
            const importDomainNextAvailable = $scope.importDomainNextAvailable();

            const act = $scope.importDomain.action;

            if (act === 'drivers' && $scope.importDomain.jdbcDriversNotFound)
                return 'Resolve issue with JDBC drivers<br>Close this dialog and try again';

            if (act === 'connect' && _.isNil($scope.selectedPreset.jdbcDriverClass))
                return 'Input valid JDBC driver class name';

            if (act === 'connect' && _.isNil($scope.selectedPreset.jdbcUrl))
                return 'Input valid JDBC URL';

            if (act === 'connect' || act === 'drivers')
                return 'Click to load list of schemas from database';

            if (act === 'schemas')
                return importDomainNextAvailable ? 'Click to load list of tables from database' : 'Select schemas to continue';

            if (act === 'tables')
                return importDomainNextAvailable ? 'Click to show import options' : 'Select tables to continue';

            if (act === 'options')
                return 'Click to import domain model for selected tables';

            return 'Click to continue';
        };

        $scope.prevTooltipText = function() {
            const act = $scope.importDomain.action;

            if (act === 'schemas')
                return $scope.importDomain.demo ? 'Click to return on demo description step' : 'Click to return on connection configuration step';

            if (act === 'tables')
                return 'Click to return on schemas selection step';

            if (act === 'options')
                return 'Click to return on tables selection step';
        };

        $scope.importDomainNextAvailable = function() {
            switch ($scope.importDomain.action) {
                case 'connect':
                    return !_.isNil($scope.selectedPreset.jdbcDriverClass) && !_.isNil($scope.selectedPreset.jdbcUrl);

                case 'schemas':
                    return _.isEmpty($scope.importDomain.schemas) || !!get('importDomain.schemasToUse.length')($scope);

                case 'tables':
                    return !!$scope.importDomain.tablesToUse.length;

                default:
                    return true;
            }
        };

        $scope.importDomainPrev = function() {
            $scope.importDomain.button = 'Next';

            if ($scope.importDomain.action === 'options') {
                $scope.importDomain.action = 'tables';
                $scope.importDomain.info = INFO_SELECT_TABLES;
            }
            else if ($scope.importDomain.action === 'tables' && $scope.importDomain.schemas.length > 0) {
                $scope.importDomain.action = 'schemas';
                $scope.importDomain.info = INFO_SELECT_SCHEMAS;
            }
            else {
                $scope.importDomain.action = 'connect';
                $scope.importDomain.info = INFO_CONNECT_TO_DB;
            }
        };

        const demo = $root.IgniteDemoMode;

        $scope.importDomain = {
            demo,
            action: demo ? 'connect' : 'drivers',
            jdbcDriversNotFound: demo,
            schemas: [],
            allSchemasSelected: false,
            tables: [],
            allTablesSelected: false,
            button: 'Next',
            info: ''
        };

        $scope.importDomain.loadingOptions = LOADING_JDBC_DRIVERS;

        agentMgr.startAgentWatch('Back', this.$uiRouter.globals.current.name)
            .then(() => {
                ActivitiesData.post({
                    group: 'configuration',
                    action: 'configuration/import/model'
                });

                return true;
            })
            .then(() => {
                if (demo) {
                    $scope.ui.packageNameUserInput = $scope.ui.packageName;
                    $scope.ui.packageName = 'model';

                    return;
                }

                // Get available JDBC drivers via agent.
                Loading.start('importDomainFromDb');

                $scope.jdbcDriverJars = [];
                $scope.ui.selectedJdbcDriverJar = {};

                return agentMgr.drivers()
                    .then((drivers) => {
                        $scope.ui.packageName = $scope.ui.packageNameUserInput;

                        if (drivers && drivers.length > 0) {
                            drivers = _.sortBy(drivers, 'jdbcDriverJar');

                            _.forEach(drivers, (drv) => {
                                $scope.jdbcDriverJars.push({
                                    label: drv.jdbcDriverJar,
                                    value: {
                                        jdbcDriverJar: drv.jdbcDriverJar,
                                        jdbcDriverClass: drv.jdbcDriverCls
                                    }
                                });
                            });

                            $scope.ui.selectedJdbcDriverJar = $scope.jdbcDriverJars[0].value;

                            $scope.importDomain.action = 'connect';
                            $scope.importDomain.tables = [];
                            this.selectedTables = [];
                        }
                        else {
                            $scope.importDomain.jdbcDriversNotFound = true;
                            $scope.importDomain.button = 'Cancel';
                        }
                    })
                    .then(() => {
                        $scope.importDomain.info = INFO_CONNECT_TO_DB;

                        Loading.finish('importDomainFromDb');
                    });
            });

        $scope.$watch('ui.selectedJdbcDriverJar', function(val) {
            if (val && !$scope.importDomain.demo) {
                const foundPreset = _findPreset(val);

                const selectedPreset = $scope.selectedPreset;

                selectedPreset.db = foundPreset.db;
                selectedPreset.jdbcDriverJar = foundPreset.jdbcDriverJar;
                selectedPreset.jdbcDriverClass = foundPreset.jdbcDriverClass;
                selectedPreset.jdbcUrl = foundPreset.jdbcUrl;
                selectedPreset.user = foundPreset.user;
            }
        }, true);
    }

    _fillCommonCachesOrTemplates(item) {
        return (action) => {
            if (item.cachesOrTemplates)
                item.cachesOrTemplates.length = 0;
            else
                item.cachesOrTemplates = [];

            if (action === IMPORT_DM_NEW_CACHE)
                item.cachesOrTemplates.push(...CACHE_TEMPLATES);

            if (!_.isEmpty(this.$scope.caches)) {
                item.cachesOrTemplates.push(...this.$scope.caches);
                this.onCacheSelect(item.cachesOrTemplates[0].value);
            }

            if (
                !_.find(item.cachesOrTemplates, {value: item.cacheOrTemplate}) &&
                item.cachesOrTemplates.length
            )
                item.cacheOrTemplate = item.cachesOrTemplates[0].value;
        };
    }

    schemasColumnDefs = [
        {
            name: 'name',
            displayName: 'Name',
            field: 'name',
            enableHiding: false,
            sort: {direction: 'asc', priority: 0},
            filter: {
                placeholder: 'Filter by Name…'
            },
            visible: true,
            sortingAlgorithm: naturalCompare,
            minWidth: 165
        }
    ];

    tablesColumnDefs = [
        {
            name: 'schema',
            displayName: 'Schema',
            field: 'schema',
            enableHiding: false,
            enableFiltering: false,
            sort: {direction: 'asc', priority: 0},
            visible: true,
            sortingAlgorithm: naturalCompare,
            minWidth: 100
        },
        {
            name: 'table',
            displayName: 'Table',
            field: 'table',
            enableHiding: false,
            enableFiltering: true,
            filter: {
                placeholder: 'Filter by Table…'
            },
            visible: true,
            sortingAlgorithm: naturalCompare,
            minWidth: 200
        },
        {
            name: 'action',
            displayName: 'Action',
            field: 'action',
            enableHiding: false,
            enableFiltering: false,
            cellTemplate: `
                <tables-action-cell
                    table='row.entity'
                    on-edit-start='grid.appScope.$ctrl.$scope.startEditDbTableCache($event)'
                    on-cache-select='grid.appScope.$ctrl.onCacheSelect($event)'
                    caches='grid.appScope.$ctrl._importCachesOrTemplates'
                    import-actions='grid.appScope.$ctrl.$scope.importActions'
                ></tables-action-cell>
            `,
            visible: true,
            minWidth: 450
        }
    ];
}

export const component = {
    name: 'modalImportModels',
    controller: ModalImportModels,
    templateUrl,
    bindings: {
        onHide: '&',
        clusterID: '<clusterId'
    }
};
