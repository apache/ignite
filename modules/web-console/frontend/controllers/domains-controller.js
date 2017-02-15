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

// Controller for Domain model screen.
export default ['domainsController', [
    '$rootScope', '$scope', '$http', '$state', '$filter', '$timeout', '$modal', 'IgniteLegacyUtils', 'IgniteMessages', 'IgniteFocus', 'IgniteConfirm', 'IgniteConfirmBatch', 'IgniteClone', 'IgniteLoading', 'IgniteModelNormalizer', 'IgniteUnsavedChangesGuard', 'IgniteAgentMonitor', 'IgniteLegacyTable', 'IgniteConfigurationResource', 'IgniteErrorPopover', 'IgniteFormUtils', 'JavaTypes', 'SqlTypes', 'IgniteActivitiesData',
    function($root, $scope, $http, $state, $filter, $timeout, $modal, LegacyUtils, Messages, Focus, Confirm, ConfirmBatch, Clone, Loading, ModelNormalizer, UnsavedChangesGuard, IgniteAgentMonitor, LegacyTable, Resource, ErrorPopover, FormUtils, JavaTypes, SqlTypes, ActivitiesData) {
        UnsavedChangesGuard.install($scope);

        const emptyDomain = {empty: true};

        let __original_value;

        const blank = {};

        // We need to initialize backupItem with empty object in order to properly used from angular directives.
        $scope.backupItem = emptyDomain;

        $scope.ui = FormUtils.formUI();
        $scope.ui.activePanels = [0, 1];
        $scope.ui.topPanels = [0, 1, 2];

        const IMPORT_DM_NEW_CACHE = 1;
        const IMPORT_DM_ASSOCIATE_CACHE = 2;

        /**
         * Convert some name to valid java package name.
         *
         * @param name to convert.
         * @returns {string} Valid java package name.
         */
        const _toJavaPackage = (name) => {
            return name ? name.replace(/[^A-Za-z_0-9/.]+/g, '_') : 'org';
        };

        const _packageNameUpdate = (event, user) => {
            if (_.isNil(user))
                return;

            $scope.ui.packageNameUserInput = _toJavaPackage(user.email.replace('@', '.').split('.').reverse().join('.') + '.model');
        };

        _packageNameUpdate(null, $root.user);

        $scope.$on('$destroy', $root.$on('user', _packageNameUpdate));

        $scope.ui.generatePojo = true;
        $scope.ui.builtinKeys = true;
        $scope.ui.usePrimitives = true;
        $scope.ui.generateAliases = true;
        $scope.ui.generatedCachesClusters = [];

        function _mapCaches(caches) {
            return _.map(caches, (cache) => {
                return {label: cache.name, value: cache._id, cache};
            });
        }

        $scope.contentVisible = function() {
            const item = $scope.backupItem;

            return !item.empty && (!item._id || _.find($scope.displayedRows, {_id: item._id}));
        };

        $scope.javaBuiltInClasses = LegacyUtils.javaBuiltInClasses;
        $scope.compactJavaName = FormUtils.compactJavaName;
        $scope.widthIsSufficient = FormUtils.widthIsSufficient;
        $scope.saveBtnTipText = FormUtils.saveBtnTipText;

        $scope.tableSave = function(field, index, stopEdit) {
            if (LegacyTable.tableEditing({model: 'table-index-fields'}, LegacyTable.tableEditedRowIndex())) {
                if ($scope.tableIndexItemSaveVisible(field, index))
                    return $scope.tableIndexItemSave(field, field.indexIdx, index, stopEdit);
            }
            else {
                switch (field.type) {
                    case 'fields':
                    case 'aliases':
                        if (LegacyTable.tablePairSaveVisible(field, index))
                            return LegacyTable.tablePairSave($scope.tablePairValid, $scope.backupItem, field, index, stopEdit) || stopEdit;

                        break;

                    case 'table-indexes':
                        if ($scope.tableIndexSaveVisible(field, index))
                            return $scope.tableIndexSave(field, index, stopEdit);

                        break;

                    case 'table-db-fields':
                        if ($scope.tableDbFieldSaveVisible(field, index))
                            return $scope.tableDbFieldSave(field, index, stopEdit);

                        break;

                    default:
                }
            }

            return true;
        };

        $scope.tableReset = (trySave) => {
            const field = LegacyTable.tableField();

            if (trySave && LegacyUtils.isDefined(field) && !$scope.tableSave(field, LegacyTable.tableEditedRowIndex(), true))
                return false;

            LegacyTable.tableReset();

            return true;
        };

        $scope.tableNewItem = function(field) {
            if ($scope.tableReset(true))
                LegacyTable.tableNewItem(field);
        };

        $scope.tableNewItemActive = LegacyTable.tableNewItemActive;

        $scope.tableStartEdit = function(item, field, index) {
            if ($scope.tableReset(true))
                LegacyTable.tableStartEdit(item, field, index, $scope.tableSave);
        };

        $scope.tableEditing = LegacyTable.tableEditing;

        $scope.tableRemove = function(item, field, index) {
            if ($scope.tableReset(true)) {
                // Remove field from indexes.
                if (field.type === 'fields') {
                    _.forEach($scope.backupItem.indexes, (modelIndex) => {
                        modelIndex.fields = _.filter(modelIndex.fields, (indexField) => {
                            return indexField.name !== $scope.backupItem.fields[index].name;
                        });
                    });
                }

                LegacyTable.tableRemove(item, field, index);
            }
        };

        $scope.tablePairSave = (pairValid, item, field, index, stopEdit) => {
            // On change of field name update that field in index fields.
            if (index >= 0 && field.type === 'fields') {
                const newName = LegacyTable.tablePairValue(field, index).key;
                const oldName = _.get(item, field.model)[index][field.keyName];

                const saved = LegacyTable.tablePairSave(pairValid, item, field, index, stopEdit);

                if (saved && oldName !== newName) {
                    _.forEach($scope.backupItem.indexes, (idx) => {
                        _.forEach(idx.fields, (fld) => {
                            if (fld.name === oldName)
                                fld.name = newName;
                        });
                    });
                }

                return saved;
            }

            return LegacyTable.tablePairSave(pairValid, item, field, index, stopEdit);
        };

        $scope.tablePairSaveVisible = LegacyTable.tablePairSaveVisible;

        $scope.queryFieldsTbl = {
            type: 'fields',
            model: 'fields',
            focusId: 'QryField',
            ui: 'table-pair',
            keyName: 'name',
            valueName: 'className',
            save: $scope.tableSave
        };

        $scope.aliasesTbl = {
            type: 'aliases',
            model: 'aliases',
            focusId: 'Alias',
            ui: 'table-pair',
            keyName: 'field',
            valueName: 'alias',
            save: $scope.tableSave
        };

        $scope.queryMetadataVariants = LegacyUtils.mkOptions(['Annotations', 'Configuration']);

        // Create list of fields to show in index fields dropdown.
        $scope.fields = (prefix, cur) => {
            const fields = _.map($scope.backupItem.fields, (field) => ({value: field.name, label: field.name}));

            if (prefix === 'new')
                return fields;

            if (cur && !_.find(fields, {value: cur}))
                fields.push({value: cur, label: cur + ' (Unknown field)'});

            return fields;
        };

        const INFO_CONNECT_TO_DB = 'Configure connection to database';
        const INFO_SELECT_SCHEMAS = 'Select schemas to load tables from';
        const INFO_SELECT_TABLES = 'Select tables to import as domain model';
        const INFO_SELECT_OPTIONS = 'Select import domain model options';
        const LOADING_JDBC_DRIVERS = {text: 'Loading JDBC drivers...'};
        const LOADING_SCHEMAS = {text: 'Loading schemas...'};
        const LOADING_TABLES = {text: 'Loading tables...'};
        const SAVING_DOMAINS = {text: 'Saving domain model...'};

        $scope.ui.invalidKeyFieldsTooltip = 'Found key types without configured key fields<br/>' +
            'It may be a result of import tables from database without primary keys<br/>' +
            'Key field for such key types should be configured manually';

        $scope.indexType = LegacyUtils.mkOptions(['SORTED', 'FULLTEXT', 'GEOSPATIAL']);

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

        $scope.ui.showValid = true;

        $scope.supportedJdbcTypes = LegacyUtils.mkOptions(LegacyUtils.SUPPORTED_JDBC_TYPES);

        $scope.supportedJavaTypes = LegacyUtils.mkOptions(LegacyUtils.javaBuiltInTypes);

        $scope.sortDirections = [
            {value: true, label: 'ASC'},
            {value: false, label: 'DESC'}
        ];

        $scope.domains = [];

        $scope.isJavaBuiltInClass = function() {
            const item = $scope.backupItem;

            if (item && item.keyType)
                return LegacyUtils.isJavaBuiltInClass(item.keyType);

            return false;
        };

        $scope.selectAllSchemas = function() {
            const allSelected = $scope.importDomain.allSchemasSelected;

            _.forEach($scope.importDomain.displayedSchemas, (schema) => {
                schema.use = allSelected;
            });
        };

        $scope.selectSchema = function() {
            if (LegacyUtils.isDefined($scope.importDomain) && LegacyUtils.isDefined($scope.importDomain.displayedSchemas))
                $scope.importDomain.allSchemasSelected = $scope.importDomain.displayedSchemas.length > 0 && _.every($scope.importDomain.displayedSchemas, 'use', true);
        };

        $scope.selectAllTables = function() {
            const allSelected = $scope.importDomain.allTablesSelected;

            _.forEach($scope.importDomain.displayedTables, function(table) {
                table.use = allSelected;
            });
        };

        $scope.selectTable = function() {
            if (LegacyUtils.isDefined($scope.importDomain) && LegacyUtils.isDefined($scope.importDomain.displayedTables))
                $scope.importDomain.allTablesSelected = $scope.importDomain.displayedTables.length > 0 && _.every($scope.importDomain.displayedTables, 'use', true);
        };

        $scope.$watch('importDomain.displayedSchemas', $scope.selectSchema);

        $scope.$watch('importDomain.displayedTables', $scope.selectTable);

        // Pre-fetch modal dialogs.
        const importDomainModal = $modal({scope: $scope, templateUrl: '/configuration/domains-import.html', show: false});

        const hideImportDomain = importDomainModal.hide;

        importDomainModal.hide = function() {
            IgniteAgentMonitor.stopWatch();

            hideImportDomain();
        };

        $scope.linkId = () => $scope.backupItem._id ? $scope.backupItem._id : 'create';

        function prepareNewItem(cacheId) {
            return {
                space: $scope.spaces[0]._id,
                generatePojo: true,
                caches: cacheId && _.find($scope.caches, {value: cacheId}) ? [cacheId] : // eslint-disable-line no-nested-ternary
                    (_.isEmpty($scope.caches) ? [] : [$scope.caches[0].value]),
                queryMetadata: 'Configuration'
            };
        }

        /**
         * Show import domain models modal.
         */
        $scope.showImportDomainModal = function() {
            LegacyTable.tableReset();

            const dirty = $scope.ui.inputForm && $scope.ui.inputForm.$dirty;

            FormUtils.confirmUnsavedChanges(dirty, function() {
                if (dirty)
                    $scope.backupItem = $scope.selectedItem ? angular.copy($scope.selectedItem) : prepareNewItem();

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

                IgniteAgentMonitor.startWatch({text: 'Back to Domain models', goal: 'import domain model from database'})
                    .then(() => {
                        ActivitiesData.post({
                            group: 'configuration',
                            action: 'configuration/import/model'
                        });

                        return true;
                    })
                    .then(importDomainModal.$promise)
                    .then(importDomainModal.show)
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

                        return IgniteAgentMonitor.drivers()
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

                                    FormUtils.confirmUnsavedChanges(dirty, () => {
                                        $scope.importDomain.action = 'connect';
                                        $scope.importDomain.tables = [];

                                        Focus.move('jdbcUrl');
                                    });
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
            });
        };

        /**
         * Load list of database schemas.
         */
        function _loadSchemas() {
            IgniteAgentMonitor.awaitAgent()
                .then(function() {
                    $scope.importDomain.loadingOptions = LOADING_SCHEMAS;
                    Loading.start('importDomainFromDb');

                    if ($root.IgniteDemoMode)
                        return IgniteAgentMonitor.schemas($scope.demoConnection);

                    const preset = $scope.selectedPreset;

                    _savePreset(preset);

                    return IgniteAgentMonitor.schemas(preset);
                })
                .then(function(schemas) {
                    $scope.importDomain.schemas = _.map(schemas, function(schema) {
                        return {use: true, name: schema};
                    });

                    $scope.importDomain.action = 'schemas';

                    if ($scope.importDomain.schemas.length === 0)
                        $scope.importDomainNext();

                    $scope.importDomain.info = INFO_SELECT_SCHEMAS;
                })
                .catch(Messages.showError)
                .then(() => Loading.finish('importDomainFromDb'));
        }

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

        let _importCachesOrTemplates = [];

        $scope.tableActionView = function(tbl) {
            const cacheName = _.find(_importCachesOrTemplates, {value: tbl.cacheOrTemplate}).label;

            if (tbl.action === IMPORT_DM_NEW_CACHE)
                return 'Create ' + tbl.generatedCacheName + ' (' + cacheName + ')';

            return 'Associate with ' + cacheName;
        };

        function isValidJavaIdentifier(s) {
            return JavaTypes.validIdentifier(s) && !JavaTypes.isKeyword(s) &&
                SqlTypes.validIdentifier(s) && !SqlTypes.isKeyword(s);
        }

        function toJavaIdentifier(name) {
            const len = name.length;

            let ident = '';

            let capitalizeNext = true;

            for (let i = 0; i < len; i++) {
                const ch = name.charAt(i);

                if (ch === ' ' || ch === '_')
                    capitalizeNext = true;
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

        function _fillCommonCachesOrTemplates(item) {
            return function(action) {
                if (item.cachesOrTemplates)
                    item.cachesOrTemplates.length = 0;
                else
                    item.cachesOrTemplates = [];

                if (action === IMPORT_DM_NEW_CACHE) {
                    item.cachesOrTemplates.push(DFLT_PARTITIONED_CACHE);
                    item.cachesOrTemplates.push(DFLT_REPLICATED_CACHE);
                }

                if (!_.isEmpty($scope.caches)) {
                    if (item.cachesOrTemplates.length > 0)
                        item.cachesOrTemplates.push(null);

                    _.forEach($scope.caches, function(cache) {
                        item.cachesOrTemplates.push(cache);
                    });
                }

                if (!_.find(item.cachesOrTemplates, {value: item.cacheOrTemplate}))
                    item.cacheOrTemplate = item.cachesOrTemplates[0].value;
            };
        }

        /**
         * Load list of database tables.
         */
        function _loadTables() {
            IgniteAgentMonitor.awaitAgent()
                .then(function() {
                    $scope.importDomain.loadingOptions = LOADING_TABLES;
                    Loading.start('importDomainFromDb');

                    $scope.importDomain.allTablesSelected = false;

                    const preset = $scope.importDomain.demo ? $scope.demoConnection : $scope.selectedPreset;

                    preset.schemas = [];

                    _.forEach($scope.importDomain.schemas, function(schema) {
                        if (schema.use)
                            preset.schemas.push(schema.name);
                    });

                    return IgniteAgentMonitor.tables(preset);
                })
                .then(function(tables) {
                    _importCachesOrTemplates = [DFLT_PARTITIONED_CACHE, DFLT_REPLICATED_CACHE].concat($scope.caches);

                    _fillCommonCachesOrTemplates($scope.importCommon)($scope.importCommon.action);

                    _.forEach(tables, function(tbl, idx) {
                        tbl.id = idx;
                        tbl.action = IMPORT_DM_NEW_CACHE;
                        tbl.generatedCacheName = toJavaClassName(tbl.tbl) + 'Cache';
                        tbl.cacheOrTemplate = DFLT_PARTITIONED_CACHE.value;
                        tbl.label = tbl.schema + '.' + tbl.tbl;
                        tbl.edit = false;
                        tbl.use = LegacyUtils.isDefined(_.find(tbl.cols, function(col) {
                            return col.key;
                        }));
                    });

                    $scope.importDomain.action = 'tables';
                    $scope.importDomain.tables = tables;
                    $scope.importDomain.info = INFO_SELECT_TABLES;
                })
                .catch(Messages.showError)
                .then(() => Loading.finish('importDomainFromDb'));
        }

        $scope.applyDefaults = function() {
            _.forEach($scope.importDomain.displayedTables, function(table) {
                table.edit = false;
                table.action = $scope.importCommon.action;
                table.cacheOrTemplate = $scope.importCommon.cacheOrTemplate;
            });
        };

        $scope._curDbTable = null;

        $scope.startEditDbTableCache = function(tbl) {
            if ($scope._curDbTable) {
                $scope._curDbTable.edit = false;

                if ($scope._curDbTable.actionWatch) {
                    $scope._curDbTable.actionWatch();

                    $scope._curDbTable.actionWatch = null;
                }
            }

            $scope._curDbTable = tbl;

            const _fillFn = _fillCommonCachesOrTemplates($scope._curDbTable);

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

        function _saveBatch(batch) {
            if (batch && batch.length > 0) {
                $scope.importDomain.loadingOptions = SAVING_DOMAINS;
                Loading.start('importDomainFromDb');

                $http.post('/api/v1/configuration/domains/save/batch', batch)
                    .then(({data}) => {
                        let lastItem;
                        const newItems = [];

                        _.forEach(_mapCaches(data.generatedCaches), function(cache) {
                            $scope.caches.push(cache);
                        });

                        _.forEach(data.savedDomains, function(savedItem) {
                            const idx = _.findIndex($scope.domains, function(domain) {
                                return domain._id === savedItem._id;
                            });

                            if (idx >= 0)
                                $scope.domains[idx] = savedItem;
                            else
                                newItems.push(savedItem);

                            lastItem = savedItem;
                        });

                        _.forEach(newItems, function(item) {
                            $scope.domains.push(item);
                        });

                        if (!lastItem && $scope.domains.length > 0)
                            lastItem = $scope.domains[0];

                        $scope.selectItem(lastItem);

                        Messages.showInfo('Domain models imported from database.');

                        $scope.ui.activePanels = [0, 1, 2];

                        $scope.ui.showValid = true;
                    })
                    .catch(Messages.showError)
                    .finally(() => {
                        Loading.finish('importDomainFromDb');

                        importDomainModal.hide();
                    });
            }
            else
                importDomainModal.hide();
        }

        function _saveDomainModel(optionsForm) {
            const generatePojo = $scope.ui.generatePojo;
            const packageName = $scope.ui.packageName;

            if (generatePojo && !LegacyUtils.checkFieldValidators({inputForm: optionsForm}))
                return false;

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

            _.forEach($scope.importDomain.tables, function(table, curIx) {
                if (table.use) {
                    const qryFields = [];
                    const indexes = [];
                    const keyFields = [];
                    const valFields = [];
                    const aliases = [];

                    const tableName = table.tbl;
                    let typeName = toJavaClassName(tableName);

                    if (_.find($scope.importDomain.tables,
                            (tbl, ix) => tbl.use && ix !== curIx && tableName === tbl.tbl)) {
                        typeName = typeName + '_' + toJavaClassName(table.schema);

                        containDup = true;
                    }

                    const valType = generatePojo ? _toJavaPackage(packageName) + '.' + typeName : tableName;

                    let _containKey = false;

                    _.forEach(table.cols, function(col) {
                        const fld = dbField(col.name, SqlTypes.findJdbcType(col.type), col.nullable, col.unsigned);

                        qryFields.push({name: fld.javaFieldName, className: fld.javaType});

                        const dbName = fld.databaseFieldName;

                        if ($scope.ui.generateAliases &&
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

                    if (table.idxs) {
                        _.forEach(table.idxs, function(idx) {
                            const fields = Object.keys(idx.fields);

                            indexes.push({
                                name: idx.name, indexType: 'SORTED', fields: _.map(fields, function(fieldName) {
                                    return {
                                        name: toJavaFieldName(fieldName),
                                        direction: idx.fields[fieldName]
                                    };
                                })
                            });
                        });
                    }

                    const domainFound = _.find($scope.domains, (domain) => domain.valueType === valType);

                    const newDomain = {
                        confirm: false,
                        skip: false,
                        space: $scope.spaces[0],
                        caches: [],
                        generatePojo
                    };

                    if (LegacyUtils.isDefined(domainFound)) {
                        newDomain._id = domainFound._id;
                        newDomain.caches = domainFound.caches;
                        newDomain.confirm = true;
                    }

                    newDomain.keyType = valType + 'Key';
                    newDomain.valueType = valType;
                    newDomain.queryMetadata = 'Configuration';
                    newDomain.databaseSchema = table.schema;
                    newDomain.databaseTable = tableName;
                    newDomain.fields = qryFields;
                    newDomain.indexes = indexes;
                    newDomain.keyFields = keyFields;
                    newDomain.aliases = aliases;
                    newDomain.valueFields = valFields;

                    // If value fields not found - copy key fields.
                    if (_.isEmpty(valFields))
                        newDomain.valueFields = keyFields.slice();

                    // Use Java built-in type for key.
                    if ($scope.ui.builtinKeys && newDomain.keyFields.length === 1) {
                        const keyField = newDomain.keyFields[0];

                        newDomain.keyType = keyField.javaType;

                        // Exclude key column from query fields and indexes.
                        newDomain.fields = _.filter(newDomain.fields, (field) => field.name !== keyField.javaFieldName);

                        _.forEach(newDomain.indexes, function(index) {
                            index.fields = _.filter(index.fields, (field) => field.name !== keyField.javaFieldName);
                        });

                        newDomain.indexes = _.filter(newDomain.indexes, (index) => !_.isEmpty(index.fields));
                    }

                    // Prepare caches for generation.
                    if (table.action === IMPORT_DM_NEW_CACHE) {
                        const template = _.find(_importCachesOrTemplates, {value: table.cacheOrTemplate});

                        const newCache = angular.copy(template.cache);

                        newDomain.newCache = newCache;

                        delete newCache._id;
                        newCache.name = typeName + 'Cache';
                        newCache.clusters = $scope.ui.generatedCachesClusters;

                        // POJO store factory is not defined in template.
                        if (!newCache.cacheStoreFactory || newCache.cacheStoreFactory.kind !== 'CacheJdbcPojoStoreFactory') {
                            const dialect = $scope.importDomain.demo ? 'H2' : $scope.selectedPreset.db;

                            newCache.cacheStoreFactory = {
                                kind: 'CacheJdbcPojoStoreFactory',
                                CacheJdbcPojoStoreFactory: {dataSourceBean: 'ds' + dialect, dialect},
                                CacheJdbcBlobStoreFactory: { connectVia: 'DataSource' }
                            };
                        }

                        if (!newCache.readThrough && !newCache.writeThrough) {
                            newCache.readThrough = true;
                            newCache.writeThrough = true;
                        }
                    }
                    else {
                        const cacheId = table.cacheOrTemplate;

                        newDomain.caches = [cacheId];

                        if (!_.includes(checkedCaches, cacheId)) {
                            const cache = _.find($scope.caches, {value: cacheId}).cache;

                            const change = LegacyUtils.autoCacheStoreConfiguration(cache, [newDomain]);

                            if (change)
                                newDomain.cacheStoreChanges = [{cacheId, change}];

                            checkedCaches.push(cacheId);
                        }
                    }

                    batch.push(newDomain);
                }
            });

            /**
             * Generate message to show on confirm dialog.
             *
             * @param meta Object to confirm.
             * @returns {string} Generated message.
             */
            function overwriteMessage(meta) {
                return '<span>' +
                    'Domain model with name &quot;' + meta.databaseTable + '&quot; already exist.<br/><br/>' +
                    'Are you sure you want to overwrite it?' +
                    '</span>';
            }

            const itemsToConfirm = _.filter(batch, (item) => item.confirm);

            function checkOverwrite() {
                if (itemsToConfirm.length > 0) {
                    ConfirmBatch.confirm(overwriteMessage, itemsToConfirm)
                        .then(() => _saveBatch(_.filter(batch, (item) => !item.skip)))
                        .catch(() => Messages.showError('Importing of domain models interrupted by user.'));
                }
                else
                    _saveBatch(batch);
            }

            function checkDuplicate() {
                if (containDup) {
                    Confirm.confirm('Some tables have the same name.<br/>' +
                            'Name of types for that tables will contain schema name too.')
                        .then(() => checkOverwrite());
                }
                else
                    checkOverwrite();
            }

            if (containKey)
                checkDuplicate();
            else {
                Confirm.confirm('Some tables have no primary key.<br/>' +
                        'You will need to configure key type and key fields for such tables after import complete.')
                    .then(() => checkDuplicate());
            }
        }

        $scope.importDomainNext = function(form) {
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
                    return _.isEmpty($scope.importDomain.schemas) || _.find($scope.importDomain.schemas, {use: true});

                case 'tables':
                    return _.find($scope.importDomain.tables, {use: true});

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

        $scope.domainModelTitle = function() {
            return $scope.ui.showValid ? 'Domain model types:' : 'Domain model types without key fields:';
        };

        function selectFirstItem() {
            if ($scope.domains.length > 0)
                $scope.selectItem($scope.domains[0]);
        }

        $scope.importActions = [{
            label: 'Create new cache by template',
            shortLabel: 'Create',
            value: IMPORT_DM_NEW_CACHE
        }];

        $scope.importCommon = {};

        // When landing on the page, get domain models and show them.
        Loading.start('loadingDomainModelsScreen');

        Resource.read()
            .then(({spaces, clusters, caches, domains}) => {
                $scope.spaces = spaces;

                $scope.clusters = _.map(clusters, (cluster) => ({
                    label: cluster.name,
                    value: cluster._id
                }));

                $scope.caches = _mapCaches(caches);

                $scope.domains = _.sortBy(domains, 'valueType');

                _.forEach($scope.clusters, (cluster) => $scope.ui.generatedCachesClusters.push(cluster.value));

                if (!_.isEmpty($scope.caches)) {
                    $scope.importActions.push({
                        label: 'Associate with existing cache',
                        shortLabel: 'Associate',
                        value: IMPORT_DM_ASSOCIATE_CACHE
                    });
                }

                $scope.$watch('importCommon.action', _fillCommonCachesOrTemplates($scope.importCommon), true);

                $scope.importCommon.action = IMPORT_DM_NEW_CACHE;

                if ($state.params.linkId)
                    $scope.createItem($state.params.linkId);
                else {
                    const lastSelectedDomain = angular.fromJson(sessionStorage.lastSelectedDomain);

                    if (lastSelectedDomain) {
                        const idx = _.findIndex($scope.domains, function(domain) {
                            return domain._id === lastSelectedDomain;
                        });

                        if (idx >= 0)
                            $scope.selectItem($scope.domains[idx]);
                        else {
                            sessionStorage.removeItem('lastSelectedDomain');

                            selectFirstItem();
                        }
                    }
                    else
                        selectFirstItem();
                }

                $scope.$watch('ui.inputForm.$valid', function(valid) {
                    if (valid && ModelNormalizer.isEqual(__original_value, $scope.backupItem))
                        $scope.ui.inputForm.$dirty = false;
                });

                $scope.$watch('backupItem', function(val) {
                    if (!$scope.ui.inputForm)
                        return;

                    const form = $scope.ui.inputForm;

                    if (form.$valid && ModelNormalizer.isEqual(__original_value, val))
                        form.$setPristine();
                    else {
                        form.$setDirty();

                        const general = form.general;

                        FormUtils.markPristineInvalidAsDirty(general.keyType);
                        FormUtils.markPristineInvalidAsDirty(general.valueType);
                    }
                }, true);

                $scope.$watch('ui.activePanels.length', () => {
                    ErrorPopover.hide();
                });
            })
            .catch(Messages.showError)
            .then(() => {
                $scope.ui.ready = true;
                $scope.ui.inputForm && $scope.ui.inputForm.$setPristine();

                Loading.finish('loadingDomainModelsScreen');
            });

        const clearFormDefaults = (ngFormCtrl) => {
            if (!ngFormCtrl)
                return;

            ngFormCtrl.$defaults = {};

            _.forOwn(ngFormCtrl, (value, key) => {
                if (value && key !== '$$parentForm' && value.constructor.name === 'FormController')
                    clearFormDefaults(value);
            });
        };

        $scope.selectItem = function(item, backup) {
            function selectItem() {
                clearFormDefaults($scope.ui.inputForm);

                LegacyTable.tableReset();

                $scope.selectedItem = item;

                try {
                    if (item && item._id)
                        sessionStorage.lastSelectedDomain = angular.toJson(item._id);
                    else
                        sessionStorage.removeItem('lastSelectedDomain');
                }
                catch (ignored) {
                    // Ignore possible errors when read from storage.
                }

                if (backup)
                    $scope.backupItem = backup;
                else if (item)
                    $scope.backupItem = angular.copy(item);
                else
                    $scope.backupItem = emptyDomain;

                $scope.backupItem = _.merge({}, blank, $scope.backupItem);

                if ($scope.ui.inputForm) {
                    $scope.ui.inputForm.$error = {};
                    $scope.ui.inputForm.$setPristine();
                }

                __original_value = ModelNormalizer.normalize($scope.backupItem);

                if (LegacyUtils.isDefined($scope.backupItem) && !LegacyUtils.isDefined($scope.backupItem.queryMetadata))
                    $scope.backupItem.queryMetadata = 'Configuration';

                if (LegacyUtils.isDefined($scope.selectedItem) && !LegacyUtils.isDefined($scope.selectedItem.queryMetadata))
                    $scope.selectedItem.queryMetadata = 'Configuration';

                if (LegacyUtils.getQueryVariable('new'))
                    $state.go('base.configuration.domains');
            }

            FormUtils.confirmUnsavedChanges($scope.backupItem && $scope.ui.inputForm && $scope.ui.inputForm.$dirty, selectItem);
        };

        // Add new domain model.
        $scope.createItem = function(cacheId) {
            if ($scope.tableReset(true)) {
                $timeout(() => {
                    FormUtils.ensureActivePanel($scope.ui, 'query');
                    FormUtils.ensureActivePanel($scope.ui, 'general', 'keyTypeInput');
                });

                $scope.selectItem(null, prepareNewItem(cacheId));
            }
        };

        function checkQueryConfiguration(item) {
            if (item.queryMetadata === 'Configuration' && LegacyUtils.domainForQueryConfigured(item)) {
                if (_.isEmpty(item.fields))
                    return ErrorPopover.show('queryFields', 'Query fields should not be empty', $scope.ui, 'query');

                const indexes = item.indexes;

                if (indexes && indexes.length > 0) {
                    if (_.find(indexes, function(index, idx) {
                        if (_.isEmpty(index.fields))
                            return !ErrorPopover.show('indexes' + idx, 'Index fields are not specified', $scope.ui, 'query');

                        if (_.find(index.fields, (field) => !_.find(item.fields, (configuredField) => configuredField.name === field.name)))
                            return !ErrorPopover.show('indexes' + idx, 'Index contains not configured fields', $scope.ui, 'query');
                    }))
                        return false;
                }
            }

            return true;
        }

        function checkStoreConfiguration(item) {
            if (LegacyUtils.domainForStoreConfigured(item)) {
                if (LegacyUtils.isEmptyString(item.databaseSchema))
                    return ErrorPopover.show('databaseSchemaInput', 'Database schema should not be empty', $scope.ui, 'store');

                if (LegacyUtils.isEmptyString(item.databaseTable))
                    return ErrorPopover.show('databaseTableInput', 'Database table should not be empty', $scope.ui, 'store');

                if (_.isEmpty(item.keyFields))
                    return ErrorPopover.show('keyFields', 'Key fields are not specified', $scope.ui, 'store');

                if (LegacyUtils.isJavaBuiltInClass(item.keyType) && item.keyFields.length !== 1)
                    return ErrorPopover.show('keyFields', 'Only one field should be specified in case when key type is a Java built-in type', $scope.ui, 'store');

                if (_.isEmpty(item.valueFields))
                    return ErrorPopover.show('valueFields', 'Value fields are not specified', $scope.ui, 'store');
            }

            return true;
        }

        // Check domain model logical consistency.
        function validate(item) {
            if (!LegacyUtils.checkFieldValidators($scope.ui))
                return false;

            if (!checkQueryConfiguration(item))
                return false;

            if (!checkStoreConfiguration(item))
                return false;

            if (!LegacyUtils.domainForStoreConfigured(item) && !LegacyUtils.domainForQueryConfigured(item) && item.queryMetadata === 'Configuration')
                return ErrorPopover.show('query-title', 'SQL query domain model should be configured', $scope.ui, 'query');

            if (!LegacyUtils.domainForStoreConfigured(item) && item.generatePojo)
                return ErrorPopover.show('store-title', 'Domain model for cache store should be configured when generation of POJO classes is enabled', $scope.ui, 'store');

            return true;
        }

        function _checkShowValidPresentation() {
            if (!$scope.ui.showValid) {
                const validFilter = $filter('domainsValidation');

                $scope.ui.showValid = validFilter($scope.domains, false, true).length === 0;
            }
        }

        // Save domain models into database.
        function save(item) {
            const qry = LegacyUtils.domainForQueryConfigured(item);
            const str = LegacyUtils.domainForStoreConfigured(item);

            item.kind = 'query';

            if (qry && str)
                item.kind = 'both';
            else if (str)
                item.kind = 'store';

            $http.post('/api/v1/configuration/domains/save', item)
                .then(({data}) => {
                    $scope.ui.inputForm.$setPristine();

                    const savedMeta = data.savedDomains[0];

                    const idx = _.findIndex($scope.domains, function(domain) {
                        return domain._id === savedMeta._id;
                    });

                    if (idx >= 0)
                        _.assign($scope.domains[idx], savedMeta);
                    else
                        $scope.domains.push(savedMeta);

                    _.forEach($scope.caches, (cache) => {
                        if (_.includes(item.caches, cache.value))
                            cache.cache.domains = _.union(cache.cache.domains, [savedMeta._id]);
                        else
                            _.pull(cache.cache.domains, savedMeta._id);
                    });

                    $scope.selectItem(savedMeta);

                    Messages.showInfo(`Domain model "${item.valueType}" saved.`);

                    _checkShowValidPresentation();
                })
                .catch(Messages.showError);
        }

        // Save domain model.
        $scope.saveItem = function() {
            if ($scope.tableReset(true)) {
                const item = $scope.backupItem;

                item.cacheStoreChanges = [];

                _.forEach(item.caches, function(cacheId) {
                    const cache = _.find($scope.caches, {value: cacheId}).cache;

                    const change = LegacyUtils.autoCacheStoreConfiguration(cache, [item]);

                    if (change)
                        item.cacheStoreChanges.push({cacheId, change});
                });

                if (validate(item))
                    save(item);
            }
        };

        function _domainNames() {
            return _.map($scope.domains, function(domain) {
                return domain.valueType;
            });
        }

        function _newNameIsValidJavaClass(newName) {
            return !$scope.backupItem.generatePojo ||
                LegacyUtils.isValidJavaClass('New name for value type', newName, false, 'copy-new-nameInput');
        }

        // Save domain model with new name.
        $scope.cloneItem = function() {
            if ($scope.tableReset(true) && validate($scope.backupItem)) {
                Clone.confirm($scope.backupItem.valueType, _domainNames(), _newNameIsValidJavaClass).then(function(newName) {
                    const item = angular.copy($scope.backupItem);

                    delete item._id;
                    item.valueType = newName;

                    save(item);
                });
            }
        };

        // Remove domain model from db.
        $scope.removeItem = function() {
            LegacyTable.tableReset();

            const selectedItem = $scope.selectedItem;

            Confirm.confirm('Are you sure you want to remove domain model: "' + selectedItem.valueType + '"?')
                .then(function() {
                    const _id = selectedItem._id;

                    $http.post('/api/v1/configuration/domains/remove', {_id})
                        .then(() => {
                            Messages.showInfo('Domain model has been removed: ' + selectedItem.valueType);

                            const domains = $scope.domains;

                            const idx = _.findIndex(domains, {_id});

                            if (idx >= 0) {
                                domains.splice(idx, 1);

                                $scope.ui.inputForm.$setPristine();

                                if (domains.length > 0)
                                    $scope.selectItem(domains[0]);
                                else
                                    $scope.backupItem = emptyDomain;

                                _.forEach($scope.caches, (cache) => _.pull(cache.cache.domains, _id));
                            }

                            _checkShowValidPresentation();
                        })
                        .catch(Messages.showError);
                });
        };

        // Remove all domain models from db.
        $scope.removeAllItems = function() {
            LegacyTable.tableReset();

            Confirm.confirm('Are you sure you want to remove all domain models?')
                .then(function() {
                    $http.post('/api/v1/configuration/domains/remove/all')
                        .then(() => {
                            Messages.showInfo('All domain models have been removed');

                            $scope.domains = [];

                            _.forEach($scope.caches, (cache) => cache.cache.domains = []);

                            $scope.backupItem = emptyDomain;
                            $scope.ui.showValid = true;
                            $scope.ui.inputForm.$error = {};
                            $scope.ui.inputForm.$setPristine();
                        })
                        .catch(Messages.showError);
                });
        };

        $scope.toggleValid = function() {
            $scope.ui.showValid = !$scope.ui.showValid;

            const validFilter = $filter('domainsValidation');

            let idx = -1;

            if (LegacyUtils.isDefined($scope.selectedItem)) {
                idx = _.findIndex(validFilter($scope.domains, $scope.ui.showValid, true), function(domain) {
                    return domain._id === $scope.selectedItem._id;
                });
            }

            if (idx === -1)
                $scope.backupItem = emptyDomain;
        };

        const pairFields = {
            fields: {
                msg: 'Query field class',
                id: 'QryField',
                idPrefix: 'Key',
                searchCol: 'name',
                valueCol: 'key',
                classValidation: true,
                dupObjName: 'name'
            },
            aliases: {id: 'Alias', idPrefix: 'Value', searchCol: 'alias', valueCol: 'value', dupObjName: 'alias'}
        };

        $scope.tablePairValid = function(item, field, index, stopEdit) {
            const pairField = pairFields[field.model];

            const pairValue = LegacyTable.tablePairValue(field, index);

            if (pairField) {
                const model = item[field.model];

                if (LegacyUtils.isDefined(model)) {
                    const idx = _.findIndex(model, function(pair) {
                        return pair[pairField.searchCol] === pairValue[pairField.valueCol];
                    });

                    // Found duplicate by key.
                    if (idx >= 0 && idx !== index)
                        return !stopEdit && ErrorPopover.show(LegacyTable.tableFieldId(index, pairField.idPrefix + pairField.id), 'Field with such ' + pairField.dupObjName + ' already exists!', $scope.ui, 'query');
                }

                if (pairField.classValidation && !LegacyUtils.isValidJavaClass(pairField.msg, pairValue.value, true, LegacyTable.tableFieldId(index, 'Value' + pairField.id), false, $scope.ui, 'query', stopEdit)) {
                    if (stopEdit)
                        return false;

                    return LegacyTable.tableFocusInvalidField(index, 'Value' + pairField.id);
                }
            }

            return true;
        };

        function tableDbFieldValue(field, index) {
            return (index < 0) ? {
                databaseFieldName: field.newDatabaseFieldName,
                databaseFieldType: field.newDatabaseFieldType,
                javaFieldName: field.newJavaFieldName,
                javaFieldType: field.newJavaFieldType
            } : {
                databaseFieldName: field.curDatabaseFieldName,
                databaseFieldType: field.curDatabaseFieldType,
                javaFieldName: field.curJavaFieldName,
                javaFieldType: field.curJavaFieldType
            };
        }

        $scope.tableDbFieldSaveVisible = function(field, index) {
            const dbFieldValue = tableDbFieldValue(field, index);

            return LegacyUtils.isDefined(dbFieldValue.databaseFieldType) &&
                LegacyUtils.isDefined(dbFieldValue.javaFieldType) &&
                !LegacyUtils.isEmptyString(dbFieldValue.databaseFieldName) &&
                !LegacyUtils.isEmptyString(dbFieldValue.javaFieldName);
        };

        const dbFieldTables = {
            keyFields: {msg: 'Key field', id: 'KeyField'},
            valueFields: {msg: 'Value field', id: 'ValueField'}
        };

        $scope.tableDbFieldSave = function(field, index, stopEdit) {
            const dbFieldTable = dbFieldTables[field.model];

            if (dbFieldTable) {
                const dbFieldValue = tableDbFieldValue(field, index);

                const item = $scope.backupItem;

                let model = item[field.model];

                if (!LegacyUtils.isValidJavaIdentifier(dbFieldTable.msg + ' java name', dbFieldValue.javaFieldName, LegacyTable.tableFieldId(index, 'JavaFieldName' + dbFieldTable.id), $scope.ui, 'store', stopEdit))
                    return stopEdit;

                if (LegacyUtils.isDefined(model)) {
                    let idx = _.findIndex(model, function(dbMeta) {
                        return dbMeta.databaseFieldName === dbFieldValue.databaseFieldName;
                    });

                    // Found duplicate.
                    if (idx >= 0 && index !== idx)
                        return stopEdit || ErrorPopover.show(LegacyTable.tableFieldId(index, 'DatabaseFieldName' + dbFieldTable.id), 'Field with such database name already exists!', $scope.ui, 'store');

                    idx = _.findIndex(model, function(dbMeta) {
                        return dbMeta.javaFieldName === dbFieldValue.javaFieldName;
                    });

                    // Found duplicate.
                    if (idx >= 0 && index !== idx)
                        return stopEdit || ErrorPopover.show(LegacyTable.tableFieldId(index, 'JavaFieldName' + dbFieldTable.id), 'Field with such java name already exists!', $scope.ui, 'store');

                    if (index < 0)
                        model.push(dbFieldValue);
                    else {
                        const dbField = model[index];

                        dbField.databaseFieldName = dbFieldValue.databaseFieldName;
                        dbField.databaseFieldType = dbFieldValue.databaseFieldType;
                        dbField.javaFieldName = dbFieldValue.javaFieldName;
                        dbField.javaFieldType = dbFieldValue.javaFieldType;
                    }
                }
                else {
                    model = [dbFieldValue];

                    item[field.model] = model;
                }

                if (!stopEdit) {
                    if (index < 0)
                        LegacyTable.tableNewItem(field);
                    else if (index < model.length - 1)
                        LegacyTable.tableStartEdit(item, field, index + 1);
                    else
                        LegacyTable.tableNewItem(field);
                }

                return true;
            }

            return false;
        };

        function tableIndexName(field, index) {
            return index < 0 ? field.newIndexName : field.curIndexName;
        }

        function tableIndexType(field, index) {
            return index < 0 ? field.newIndexType : field.curIndexType;
        }

        $scope.tableIndexSaveVisible = function(field, index) {
            return !LegacyUtils.isEmptyString(tableIndexName(field, index)) && LegacyUtils.isDefined(tableIndexType(field, index));
        };

        $scope.tableIndexSave = function(field, curIdx, stopEdit) {
            const indexName = tableIndexName(field, curIdx);
            const indexType = tableIndexType(field, curIdx);

            const item = $scope.backupItem;

            const indexes = item.indexes;

            if (LegacyUtils.isDefined(indexes)) {
                const idx = _.findIndex(indexes, function(index) {
                    return index.name === indexName;
                });

                // Found duplicate.
                if (idx >= 0 && idx !== curIdx)
                    return !stopEdit && ErrorPopover.show(LegacyTable.tableFieldId(curIdx, 'IndexName'), 'Index with such name already exists!', $scope.ui, 'query');
            }

            LegacyTable.tableReset();

            if (curIdx < 0) {
                const newIndex = {name: indexName, indexType};

                if (item.indexes)
                    item.indexes.push(newIndex);
                else
                    item.indexes = [newIndex];
            }
            else {
                item.indexes[curIdx].name = indexName;
                item.indexes[curIdx].indexType = indexType;
            }

            if (!stopEdit) {
                if (curIdx < 0)
                    $scope.tableIndexNewItem(field, item.indexes.length - 1);
                else {
                    const index = item.indexes[curIdx];

                    if (index.fields && index.fields.length > 0)
                        $scope.tableIndexItemStartEdit(field, curIdx, 0);
                    else
                        $scope.tableIndexNewItem(field, curIdx);
                }
            }

            return true;
        };

        $scope.tableIndexNewItem = function(field, indexIdx) {
            if ($scope.tableReset(true)) {
                LegacyTable.tableState(field, -1, 'table-index-fields');
                LegacyTable.tableFocusInvalidField(-1, 'FieldName' + indexIdx);

                field.newFieldName = null;
                field.newDirection = true;
                field.indexIdx = indexIdx;
            }
        };

        $scope.tableIndexNewItemActive = function(field, itemIndex) {
            const indexes = $scope.backupItem.indexes;

            if (indexes) {
                const index = indexes[itemIndex];

                if (index)
                    return LegacyTable.tableNewItemActive({model: 'table-index-fields'}) && field.indexIdx === itemIndex;
            }

            return false;
        };

        $scope.tableIndexItemEditing = function(field, itemIndex, curIdx) {
            const indexes = $scope.backupItem.indexes;

            if (indexes) {
                const index = indexes[itemIndex];

                if (index)
                    return LegacyTable.tableEditing({model: 'table-index-fields'}, curIdx) && field.indexIdx === itemIndex;
            }

            return false;
        };

        function tableIndexItemValue(field, index) {
            return index < 0 ? {
                name: field.newFieldName,
                direction: field.newDirection
            } : {
                name: field.curFieldName,
                direction: field.curDirection
            };
        }

        $scope.tableIndexItemStartEdit = function(field, indexIdx, curIdx) {
            if ($scope.tableReset(true)) {
                const index = $scope.backupItem.indexes[indexIdx];

                LegacyTable.tableState(field, curIdx, 'table-index-fields');

                const indexItem = index.fields[curIdx];

                field.curFieldName = indexItem.name;
                field.curDirection = indexItem.direction;
                field.indexIdx = indexIdx;

                Focus.move('curFieldName' + field.indexIdx + '-' + curIdx);
            }
        };

        $scope.tableIndexItemSaveVisible = function(field, index) {
            return !LegacyUtils.isEmptyString(tableIndexItemValue(field, index).name);
        };

        $scope.tableIndexItemSave = function(field, indexIdx, curIdx, stopEdit) {
            const indexItemValue = tableIndexItemValue(field, curIdx);

            const index = $scope.backupItem.indexes[indexIdx];

            const fields = index.fields;

            if (LegacyUtils.isDefined(fields)) {
                const idx = _.findIndex(fields, (fld) => fld.name === indexItemValue.name);

                // Found duplicate.
                if (idx >= 0 && idx !== curIdx) {
                    return !stopEdit && ErrorPopover.show(LegacyTable.tableFieldId(curIdx,
                        'FieldName' + indexIdx + (curIdx >= 0 ? '-' : '')),
                        'Field with such name already exists in index!', $scope.ui, 'query');
                }
            }

            LegacyTable.tableReset();

            field.indexIdx = -1;

            if (curIdx < 0) {
                if (index.fields)
                    index.fields.push(indexItemValue);
                else
                    index.fields = [indexItemValue];

                if (!stopEdit)
                    $scope.tableIndexNewItem(field, indexIdx);
            }
            else {
                index.fields[curIdx] = indexItemValue;

                if (!stopEdit) {
                    if (curIdx < index.fields.length - 1)
                        $scope.tableIndexItemStartEdit(field, indexIdx, curIdx + 1);
                    else
                        $scope.tableIndexNewItem(field, indexIdx);
                }
            }

            return true;
        };

        $scope.tableRemoveIndexItem = function(index, curIdx) {
            LegacyTable.tableReset();

            index.fields.splice(curIdx, 1);
        };

        $scope.resetAll = function() {
            LegacyTable.tableReset();

            Confirm.confirm('Are you sure you want to undo all changes for current domain model?')
                .then(function() {
                    $scope.backupItem = $scope.selectedItem ? angular.copy($scope.selectedItem) : prepareNewItem();
                    $scope.ui.inputForm.$error = {};
                    $scope.ui.inputForm.$setPristine();
                });
        };
    }
]];
