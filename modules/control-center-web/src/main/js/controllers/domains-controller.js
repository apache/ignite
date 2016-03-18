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
consoleModule.controller('domainsController', [
    '$scope', '$http', '$state', '$filter', '$timeout', '$modal', '$common', '$focus', '$confirm', '$confirmBatch', '$clone', '$table', '$preview', '$loading', '$unsavedChangesGuard', 'IgniteAgentMonitor',
    function ($scope, $http, $state, $filter, $timeout, $modal, $common, $focus, $confirm, $confirmBatch, $clone, $table, $preview, $loading, $unsavedChangesGuard, IgniteAgentMonitor) {
        $unsavedChangesGuard.install($scope);

        $scope.ui = $common.formUI();
        $scope.ui.activePanels = [0, 1];
        $scope.ui.topPanels = [];

        var IMPORT_DM_NEW_CACHE = 1;
        var IMPORT_DM_ASSOCIATE_CACHE = 2;

        $scope.ui.packageName = $commonUtils.toJavaPackageName($scope.$root.user.email.replace('@', '.')
            .split('.').reverse().join('.') + '.model');
        $scope.ui.builtinKeys = true;
        $scope.ui.usePrimitives = true;
        $scope.ui.generatedCachesClusters = [];

        $scope.removeDemoDropdown = [{
            'text': 'Remove generated demo data',
            'click': 'removeDemoItems()'
        }];

        function restoreSelection() {
            var lastSelectedDomain = angular.fromJson(sessionStorage.lastSelectedDomain);

            if (lastSelectedDomain) {
                var idx = _.findIndex($scope.domains, function (domain) {
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

        function _mapCaches(caches) {
            return _.map(caches, function (cache) {
                return {
                    label: cache.name,
                    value: cache._id,
                    cache: cache
                }
            });
        }

        $scope.removeDemoItems = function () {
            $table.tableReset();

            $confirm.confirm('Are you sure you want to remove all generated demo domain models and caches?')
                .then(function () {
                    $http.post('/api/v1/configuration/domains/remove/demo')
                        .success(function () {
                            $common.showInfo('All demo domain models and caches have been removed');

                            $http.post('/api/v1/configuration/domains/list')
                                .success(function (data) {
                                    $scope.spaces = data.spaces;
                                    $scope.clusters = _.map(data.clusters, function (cluster) {
                                        return {
                                            value: cluster._id,
                                            label: cluster.name
                                        };
                                    });
                                    $scope.caches = _mapCaches(data.caches);
                                    $scope.domains = data.domains;

                                    $scope.ui.generatedCachesClusters = [];

                                    _.forEach($scope.clusters, function (cluster) {
                                        $scope.ui.generatedCachesClusters.push(cluster.value);
                                    });

                                    restoreSelection();
                                });
                        })
                        .error(function (errMsg) {
                            $common.showError(errMsg);
                        });
                });
        };

        $scope.hasDemoItems = function () {
            return _.findIndex($scope.domains, function (domain) {
                    return domain.demo;
                }) >= 0;
        };

        $scope.joinTip = $common.joinTip;
        $scope.getModel = $common.getModel;
        $scope.javaBuiltInClasses = $common.javaBuiltInClasses;
        $scope.compactJavaName = $common.compactJavaName;
        $scope.widthIsSufficient = $common.widthIsSufficient;
        $scope.saveBtnTipText = $common.saveBtnTipText;
        $scope.panelExpanded = $common.panelExpanded;

        $scope.tableVisibleRow = $table.tableVisibleRow;

        $scope.tableSave = function (field, index, stopEdit) {
            if ($table.tableEditing({model: 'table-index-fields'}, $table.tableEditedRowIndex())) {
                if ($scope.tableIndexItemSaveVisible(field, index))
                    return $scope.tableIndexItemSave(field, field.indexIdx, index, stopEdit);
            }
            else {
                switch (field.type) {
                    case 'fields':
                    case 'aliases':
                        if ($table.tablePairSaveVisible(field, index))
                            return $table.tablePairSave($scope.tablePairValid, $scope.backupItem, field, index, stopEdit);

                        break;

                    case 'indexes':
                        if ($scope.tableIndexSaveVisible(field, index))
                            return $scope.tableIndexSave(field, index, stopEdit);

                        break;

                    case 'table-db-fields':
                        if ($scope.tableDbFieldSaveVisible(field, index))
                            return $scope.tableDbFieldSave(field, index, stopEdit);

                        break;
                }
            }

            return true;
        };

        $scope.tableReset = function (save) {
            var field = $table.tableField();

            if (!save || !$common.isDefined(field) || $scope.tableSave(field, $table.tableEditedRowIndex(), true)) {
                $table.tableReset();

                return true;
            }

            return false;
        };

        $scope.tableNewItem = function (field) {
            if ($scope.tableReset(true))
                $table.tableNewItem(field);
        };

        $scope.tableNewItemActive = $table.tableNewItemActive;

        $scope.tableStartEdit = function (item, field, index) {
            if ($scope.tableReset(true))
                $table.tableStartEdit(item, field, index);
        };

        $scope.tableEditing = $table.tableEditing;

        $scope.tableRemove = function (item, field, index) {
            if ($scope.tableReset(true))
                $table.tableRemove(item, field, index);
        };

        $scope.tableSimpleSave = $table.tableSimpleSave;
        $scope.tableSimpleSaveVisible = $table.tableSimpleSaveVisible;

        $scope.tableSimpleUp = function (item, field, index) {
            if ($scope.tableReset(true))
                $table.tableSimpleUp(item, field, index);
        };

        $scope.tableSimpleDown = function (item, field, index) {
            if ($scope.tableReset(true))
                $table.tableSimpleDown(item, field, index);
        };

        $scope.tableSimpleDownVisible = $table.tableSimpleDownVisible;

        $scope.tablePairStartEdit = $table.tablePairStartEdit;
        $scope.tablePairSave = $table.tablePairSave;
        $scope.tablePairSaveVisible = $table.tablePairSaveVisible;

        $scope.queryMetadataVariants = $common.mkOptions(['Annotations', 'Configuration']);

        var INFO_CONNECT_TO_DB = 'Configure connection to database';
        var INFO_SELECT_SCHEMAS = 'Select schemas to load tables from';
        var INFO_SELECT_TABLES = 'Select tables to import as domain model';
        var INFO_SELECT_OPTIONS = 'Select import domain model options';
        var LOADING_JDBC_DRIVERS = {text: 'Loading JDBC drivers...'};
        var LOADING_SCHEMAS = {text: 'Loading schemas...'};
        var LOADING_TABLES = {text: 'Loading tables...'};
        var SAVING_DOMAINS = {text: 'Saving domain model...'};

        $scope.ui.invalidKeyFieldsTooltip = 'Found key types without configured key fields<br/>' +
            'It may be a result of import tables from database without primary keys<br/>' +
            'Key field for such key types should be configured manually';

        var previews = [];

        $scope.previewInit = function (preview) {
            previews.push(preview);

            $preview.previewInit(preview);
        };

        $scope.previewChanged = $preview.previewChanged;

        $scope.hidePopover = $common.hidePopover;

        var showPopoverMessage = $common.showPopoverMessage;

        $scope.preview = {
            general: {xml: '', java: '', allDefaults: true},
            query: {xml: '', java: '', allDefaults: true},
            store: {xml: '', java: '', allDefaults: true}
        };

        $scope.indexType = $common.mkOptions(['SORTED', 'FULLTEXT', 'GEOSPATIAL']);

        var _dbPresets = [
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
                db: 'H2',
                jdbcDriverClass: 'org.h2.Driver',
                jdbcUrl: 'jdbc:h2:tcp://[host]/[database]',
                user: 'sa'
            }
        ];

        $scope.selectedPreset = {
            db: 'General',
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
                var restoredPresets = JSON.parse(localStorage.dbPresets);

                _.forEach(restoredPresets, function (restoredPreset) {
                    var preset = _.find(_dbPresets, {jdbcDriverClass: restoredPreset.jdbcDriverClass});

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
                var oldPreset = _.find(_dbPresets, {jdbcDriverClass: preset.jdbcDriverClass});

                if (oldPreset)
                    angular.extend(oldPreset, preset);
                else
                    _dbPresets.push(preset);

                localStorage.dbPresets = JSON.stringify(_dbPresets);
            }
            catch (errMsg) {
                $common.showError(errMsg);
            }
        }

        $scope.$watch('ui.selectedJdbcDriverJar', function (val) {
            if (val && !$scope.importDomain.demo) {
                var foundPreset = _findPreset(val);

                var selectedPreset = $scope.selectedPreset;

                selectedPreset.db = foundPreset.db;
                selectedPreset.jdbcDriverJar = foundPreset.jdbcDriverJar;
                selectedPreset.jdbcDriverClass = foundPreset.jdbcDriverClass;
                selectedPreset.jdbcUrl = foundPreset.jdbcUrl;
                selectedPreset.user = foundPreset.user;
            }
        }, true);

        $scope.ui.showValid = true;

        function _findPreset(selectedJdbcJar) {
            var result = _.find(_dbPresets, function (preset) {
                return preset.jdbcDriverClass === selectedJdbcJar.jdbcDriverClass;
            });

            if (!result)
                result = {
                    db: 'General',
                    jdbcUrl: 'jdbc:[database]',
                    user: 'admin'
                };

            result.jdbcDriverJar = selectedJdbcJar.jdbcDriverJar;
            result.jdbcDriverClass = selectedJdbcJar.jdbcDriverClass;

            return result;
        }

        $scope.supportedJdbcTypes = $common.mkOptions($common.SUPPORTED_JDBC_TYPES);

        $scope.supportedJavaTypes = $common.mkOptions($common.javaBuiltInTypes);

        $scope.sortDirections = [
            {value: true, label: 'ASC'},
            {value: false, label: 'DESC'}
        ];

        $scope.domains = [];

        $scope.isJavaBuiltInClass = function () {
            var item = $scope.backupItem;

            if (item && item.keyType)
                return $common.isJavaBuiltInClass(item.keyType);

            return false;
        };

        $scope.selectAllSchemas = function () {
            var allSelected = $scope.importDomain.allSchemasSelected;

            _.forEach($scope.importDomain.displayedSchemas, function (schema) {
                schema.use = allSelected;
            });
        };

        $scope.selectSchema = function () {
            if ($common.isDefined($scope.importDomain) && $common.isDefined($scope.importDomain.displayedSchemas))
                $scope.importDomain.allSchemasSelected = $scope.importDomain.displayedSchemas.length > 0 &&
                    _.every($scope.importDomain.displayedSchemas, 'use', true);
        };

        $scope.selectAllTables = function () {
            var allSelected = $scope.importDomain.allTablesSelected;

            _.forEach($scope.importDomain.displayedTables, function (table) {
                table.use = allSelected;
            });
        };

        $scope.selectTable = function () {
            if ($common.isDefined($scope.importDomain) && $common.isDefined($scope.importDomain.displayedTables))
                $scope.importDomain.allTablesSelected = $scope.importDomain.displayedTables.length > 0 &&
                    _.every($scope.importDomain.displayedTables, 'use', true);
        };

        $scope.$watch('importDomain.displayedSchemas', $scope.selectSchema);

        $scope.$watch('importDomain.displayedTables', $scope.selectTable);

        // Pre-fetch modal dialogs.
        var importDomainModal = $modal({scope: $scope, templateUrl: '/configuration/domains-import.html', show: false});

        var hideImportDomain = importDomainModal.hide;

        importDomainModal.hide = function () {
            IgniteAgentMonitor.stopWatch();

            hideImportDomain();
        };

        /**
         * Show import domain models modal.
         *
         * @param demo If 'true' then import domain models from demo database.
         */
        $scope.showImportDomainModal = function (demo) {
            $table.tableReset();

            $common.confirmUnsavedChanges($scope.ui.isDirty(), function () {
                if ($scope.ui.isDirty())
                    $scope.backupItem = $scope.selectedItem ? angular.copy($scope.selectedItem) : prepareNewItem();

                $scope.importDomain = {
                    demo: demo,
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

                IgniteAgentMonitor.startWatch({
                        state: 'base.configuration.domains',
                        text: 'Back to Domain models',
                        goal: 'import domain model from database schema'
                    })
                    .then(function() {
                        importDomainModal.$promise.then(importDomainModal.show);

                        if (demo) {
                            $scope.ui.packageNameUserInput = $scope.ui.packageName;
                            $scope.ui.packageName = 'model';

                            return;
                        }

                        // Get available JDBC drivers via agent.
                        $loading.start('importDomainFromDb');

                        $scope.jdbcDriverJars = [];
                        $scope.ui.selectedJdbcDriverJar = {};

                        return IgniteAgentMonitor.drivers()
                            .then(function(drivers) {
                                $scope.ui.packageName = $scope.ui.packageNameUserInput;

                                if (drivers && drivers.length > 0) {
                                    drivers = _.sortBy(drivers, 'jdbcDriverJar');

                                    drivers.forEach(function (drv) {
                                        $scope.jdbcDriverJars.push({
                                            label: drv.jdbcDriverJar,
                                            value: {
                                                jdbcDriverJar: drv.jdbcDriverJar,
                                                jdbcDriverClass: drv.jdbcDriverCls
                                            }
                                        });
                                    });

                                    $scope.ui.selectedJdbcDriverJar = $scope.jdbcDriverJars[0].value;

                                    $common.confirmUnsavedChanges($scope.ui.isDirty(), function () {
                                        importDomainModal.$promise.then(function () {
                                            $scope.importDomain.action = 'connect';
                                            $scope.importDomain.tables = [];
                                            $scope.importDomain.loadingOptions = LOADING_SCHEMAS;

                                            $focus('jdbcUrl');
                                        });
                                    });
                                }
                                else {
                                    $scope.importDomain.jdbcDriversNotFound = true;
                                    $scope.importDomain.button = 'Cancel';
                                }
                            })
                            .finally(function () {
                                $scope.importDomain.info = INFO_CONNECT_TO_DB;

                                $loading.finish('importDomainFromDb');
                            });
                    })
            });
        };

        /**
         * Load list of database schemas.
         */
        function _loadSchemas() {
            IgniteAgentMonitor.awaitAgent()
                .then(function() {
                    $loading.start('importDomainFromDb');

                    var preset = $scope.importDomain.demo ? $scope.demoConnection : $scope.selectedPreset;

                    if (!$scope.importDomain.demo)
                        _savePreset(preset);

                    return IgniteAgentMonitor.schemas(preset)
                })
                .then(function(schemas) {
                    $scope.importDomain.schemas = _.map(schemas, function (schema) {
                        return {use: false, name: schema};
                    });

                    $scope.importDomain.action = 'schemas';

                    if ($scope.importDomain.schemas.length === 0)
                        $scope.importDomainNext();
                    else
                        _.forEach($scope.importDomain.schemas, function (sch) {
                            sch.use = true;
                        });

                    $scope.importDomain.info = INFO_SELECT_SCHEMAS;
                    $scope.importDomain.loadingOptions = LOADING_TABLES;
                })
                .catch(function(errMsg) {
                    $common.showError(errMsg);
                })
                .finally(function () {
                    $loading.finish('importDomainFromDb');
                });
        }

        var DFLT_PARTITIONED_CACHE = {
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

        var DFLT_REPLICATED_CACHE = {
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

        $scope.tableActionView = function(tbl) {
            var cacheName = _.find(_importCachesOrTemplates, {value: tbl.cacheOrTemplate}).label;

            if (tbl.action === IMPORT_DM_NEW_CACHE)
                return 'Create ' + tbl.generatedCacheName + ' (' + cacheName + ')';

            return 'Associate with ' + cacheName;
        };

        /**
         * Load list of database tables.
         */
        function _loadTables() {
            IgniteAgentMonitor.awaitAgent()
                .then(function() {
                    $loading.start('importDomainFromDb');

                    $scope.importDomain.allTablesSelected = false;

                    var preset = $scope.importDomain.demo ? $scope.demoConnection : $scope.selectedPreset;

                    preset.schemas = [];

                    _.forEach($scope.importDomain.schemas, function (schema) {
                        if (schema.use)
                            preset.schemas.push(schema.name);
                    });

                    return IgniteAgentMonitor.tables(preset);
                })
                .then(function (tables) {
                    _importCachesOrTemplates = [DFLT_PARTITIONED_CACHE, DFLT_REPLICATED_CACHE].concat($scope.caches);

                    _fillCommonCachesOrTemplates($scope.importCommon)($scope.importCommon.action);

                    _.forEach(tables, function (tbl, idx) {
                        tbl.id = idx;
                        tbl.action = IMPORT_DM_NEW_CACHE;
                        tbl.generatedCacheName = toJavaClassName(tbl.tbl) + 'Cache';
                        tbl.cacheOrTemplate = DFLT_PARTITIONED_CACHE.value;
                        tbl.label = tbl.schema + '.' + tbl.tbl;
                        tbl.edit = false;
                        tbl.use = $common.isDefined(_.find(tbl.cols, function (col) {
                            return col.key;
                        }));
                    });

                    $scope.importDomain.action = 'tables';
                    $scope.importDomain.tables = tables;
                    $scope.importDomain.info = INFO_SELECT_TABLES;
                })
                .catch(function(errMsg) {
                    $common.showError(errMsg);
                })
                .finally(function () {
                    $loading.finish('importDomainFromDb');
                });
        }

        $scope.applyDefaults = function () {
            _.forEach($scope.importDomain.displayedTables, function (table) {
                table.edit = false;
                table.action = $scope.importCommon.action;
                table.cacheOrTemplate = $scope.importCommon.cacheOrTemplate;
            });
        };

        $scope._curDbTable = null;

        $scope.startEditDbTableCache = function (tbl) {
            if ($scope._curDbTable) {
                $scope._curDbTable.edit = false;

                if ($scope._curDbTable.actionWatch) {
                    $scope._curDbTable.actionWatch();

                    $scope._curDbTable.actionWatch = null;
                }
            }

            $scope._curDbTable = tbl;

            var _fillFn = _fillCommonCachesOrTemplates($scope._curDbTable);

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
            $scope.importDomain.loadingOptions = SAVING_DOMAINS;
        }

        function toJavaClassName(name) {
            var len = name.length;

            var buf = '';

            var capitalizeNext = true;

            for (var i = 0; i < len; i++) {
                var ch = name.charAt(i);

                if (ch === ' ' || ch === '_')
                    capitalizeNext = true;
                else if (capitalizeNext) {
                    buf += ch.toLocaleUpperCase();

                    capitalizeNext = false;
                }
                else
                    buf += ch.toLocaleLowerCase();
            }

            return buf;
        }

        function toJavaName(dbName) {
            var javaName = toJavaClassName(dbName);

            return javaName.charAt(0).toLocaleLowerCase() + javaName.slice(1);
        }

        function _saveBatch(batch) {
            if (batch && batch.length > 0) {
                $loading.start('importDomainFromDb');

                $http.post('/api/v1/configuration/domains/save/batch', batch)
                    .success(function (savedBatch) {
                        var lastItem;
                        var newItems = [];

                        _.forEach(_mapCaches(savedBatch.generatedCaches), function(cache) {
                            $scope.caches.push(cache);
                        });

                        _.forEach(savedBatch.savedDomains, function (savedItem) {
                            var idx = _.findIndex($scope.domains, function (domain) {
                                return domain._id === savedItem._id;
                            });

                            if (idx >= 0)
                                $scope.domains[idx] = savedItem;
                            else
                                newItems.push(savedItem);

                            lastItem = savedItem;
                        });

                        _.forEach(newItems, function (item) {
                            $scope.domains.push(item);
                        });

                        if (!lastItem && $scope.domains.length > 0)
                            lastItem = $scope.domains[0];

                        $scope.selectItem(lastItem);

                        $common.showInfo('Domain models imported from database.');

                        $scope.ui.activePanels = [0, 1, 2];

                        $scope.ui.showValid = true;
                    })
                    .error(function (errMsg) {
                        $common.showError(errMsg);
                    })
                    .finally(function () {
                        $loading.finish('importDomainFromDb');

                        importDomainModal.hide();
                    });
            }
            else
                importDomainModal.hide();
        }

        function _saveDomainModel() {
            if ($common.isEmptyString($scope.ui.packageName))
                return $common.showPopoverMessage(undefined, undefined, 'domainPackageName',
                    'Package should be not empty');

            if (!$common.isValidJavaClass('Package', $scope.ui.packageName, false, 'domainPackageName', true))
                return false;

            var batch = [];
            var tables = [];
            var checkedCaches = [];
            var dupCnt = 0;

            var containKey = true;

            function queryField(name, jdbcType) {
                return {name: toJavaName(name), className: jdbcType.javaType};
            }

            function dbField(name, jdbcType, nullable) {
                return {
                    jdbcType: jdbcType,
                    databaseFieldName: name,
                    databaseFieldType: jdbcType.dbName,
                    javaFieldName: toJavaName(name),
                    javaFieldType: nullable ? jdbcType.javaType :
                        ($scope.ui.usePrimitives && jdbcType.primitiveType ? jdbcType.primitiveType : jdbcType.javaType)
                };
            }

            _.forEach($scope.importDomain.tables, function (table) {
                if (table.use) {
                    var qryFields = [];
                    var indexes = [];
                    var keyFields = [];
                    var valFields = [];

                    var tableName = table.tbl;

                    var dup = tables.indexOf(tableName) >= 0;

                    if (dup)
                        dupCnt++;

                    var typeName = toJavaClassName(tableName);
                    var valType = $commonUtils.toJavaPackageName($scope.ui.packageName) + '.' + typeName;

                    var _containKey = false;

                    _.forEach(table.cols, function (col) {
                        var colName = col.name;
                        var jdbcType = $common.findJdbcType(col.type);
                        var nullable = col.nullable;

                        qryFields.push(queryField(colName, jdbcType));

                        if (col.key) {
                            keyFields.push(dbField(colName, jdbcType, nullable));

                            _containKey = true;
                        }
                        else
                            valFields.push(dbField(colName, jdbcType, nullable));
                    });

                    containKey &= _containKey;

                    if (table.idxs) {
                        _.forEach(table.idxs, function (idx) {
                            var fields = Object.keys(idx.fields);

                            indexes.push({
                                name: idx.name, indexType: 'SORTED', fields: _.map(fields, function (fieldName) {
                                    return {
                                        name: toJavaName(fieldName),
                                        direction: idx.fields[fieldName]
                                    };
                                })
                            });
                        });
                    }

                    var domainFound = _.find($scope.domains, function (domain) {
                        return domain.valueType === valType;
                    });

                    var newDomain = {
                        confirm: false,
                        skip: false,
                        space: $scope.spaces[0],
                        caches: []
                    };

                    if ($common.isDefined(domainFound)) {
                        newDomain._id = domainFound._id;
                        newDomain.caches = domainFound.caches;
                        newDomain.confirm = true;
                    }

                    var dupSfx = (dup ? '_' + dupCnt : '');

                    newDomain.keyType = valType + 'Key' + dupSfx;
                    newDomain.valueType = valType + dupSfx;
                    newDomain.queryMetadata = 'Configuration';
                    newDomain.databaseSchema = table.schema;
                    newDomain.databaseTable = tableName;
                    newDomain.fields = qryFields;
                    newDomain.indexes = indexes;
                    newDomain.keyFields = keyFields;
                    newDomain.valueFields = valFields;
                    newDomain.demo = $scope.importDomain.demo;

                    // If value fields not found - copy key fields.
                    if ($common.isEmptyArray(valFields))
                        newDomain.valueFields = keyFields.slice();

                    // Use Java built-in type for key.
                    if ($scope.ui.builtinKeys && newDomain.keyFields.length === 1) {
                        var keyField = newDomain.keyFields[0];

                        newDomain.keyType = keyField.jdbcType.javaType;

                        // Exclude key column from query fields and indexes.
                        newDomain.fields = _.filter(newDomain.fields, function (field) {
                            return field.name != keyField.javaFieldName;
                        });

                        _.forEach(newDomain.indexes, function (index) {
                            index.fields = _.filter(index.fields, function (field) {
                                return field.name !== keyField.javaFieldName;
                            })
                        });

                        newDomain.indexes = _.filter(newDomain.indexes, function (index) {
                            return !$common.isEmptyArray(index.fields);
                        });
                    }

                    // Prepare caches for generation.
                    if (table.action === IMPORT_DM_NEW_CACHE) {
                        var template = _.find(_importCachesOrTemplates, {value: table.cacheOrTemplate});

                        newDomain.newCache = angular.copy(template.cache);

                        delete newDomain.newCache._id;
                        newDomain.newCache.name = typeName + 'Cache';
                        newDomain.newCache.clusters = $scope.ui.generatedCachesClusters;
                        newDomain.newCache.demo = $scope.importDomain.demo;

                        // POJO store factory is not defined in template.
                        if (!newDomain.newCache.cacheStoreFactory ||
                            newDomain.newCache.cacheStoreFactory.kind !== 'CacheJdbcPojoStoreFactory') {
                            var dialect = $scope.importDomain.demo ? 'H2' : $scope.selectedPreset.db;

                            newDomain.newCache.cacheStoreFactory = {
                                kind: 'CacheJdbcPojoStoreFactory',
                                CacheJdbcPojoStoreFactory: {
                                    dataSourceBean: 'ds' + dialect,
                                    dialect: dialect
                                }
                            };
                        }
                    }
                    else {
                        var cacheId = table.cacheOrTemplate;

                        newDomain.caches = [cacheId];

                        if (!_.contains(checkedCaches, cacheId)) {
                            var cache = _.find($scope.caches, {value: cacheId}).cache;

                            var change = $common.autoCacheStoreConfiguration(cache, [newDomain]);

                            if (change)
                                newDomain.cacheStoreChanges = [{cacheId: cacheId, change: change}];

                            checkedCaches.push(cacheId)
                        }
                    }

                    batch.push(newDomain);
                    tables.push(tableName);
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

            var itemsToConfirm = _.filter(batch, function (item) {
                return item.confirm;
            });

            function checkOverwrite() {
                if (itemsToConfirm.length > 0)
                    $confirmBatch.confirm(overwriteMessage, itemsToConfirm)
                        .then(function () {
                            _saveBatch(_.filter(batch, function (item) {
                                return !item.skip;
                            }));
                        })
                        .catch(function () {
                            $common.showError('Importing of domain models interrupted by user.');
                        });
                else
                    _saveBatch(batch);
            }

            if (containKey)
                checkOverwrite();
            else
                $confirm.confirm('Some tables have no primary key.<br/>' +
                        'You will need to configure key type and key fields for such tables after import complete.')
                    .then(function () {
                        checkOverwrite();
                    });
        }

        $scope.importDomainNext = function () {
            if (!$scope.importDomainNextAvailable())
                return;

            var act = $scope.importDomain.action;

            if (act === 'drivers' && $scope.importDomain.jdbcDriversNotFound)
                importDomainModal.hide();
            else if (act === 'connect')
                _loadSchemas();
            else if (act === 'schemas')
                _loadTables();
            else if (act === 'tables')
                _selectOptions();
            else if (act === 'options')
                _saveDomainModel();
        };

        $scope.nextTooltipText = function () {
            var importDomainNextAvailable = $scope.importDomainNextAvailable();

            var act = $scope.importDomain.action;

            if (act === 'drivers' && $scope.importDomain.jdbcDriversNotFound)
                return 'Resolve issue with JDBC drivers<br>Close this dialog and try again';

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

        $scope.prevTooltipText = function () {
            var act = $scope.importDomain.action;

            if (act === 'schemas')
                return $scope.importDomain.demo ? 'Click to return on demo description step' : 'Click to return on connection configuration step';

            if (act === 'tables')
                return 'Click to return on schemas selection step';

            if (act === 'options')
                return 'Click to return on tables selection step';
        };

        $scope.importDomainNextAvailable = function () {
            var res = true;

            switch ($scope.importDomain.action) {
                case 'schemas':
                    res = $common.isEmptyArray($scope.importDomain.schemas) ||
                        _.find($scope.importDomain.schemas, {use: true});

                    break;

                case 'tables':
                    res = _.find($scope.importDomain.tables, {use: true});

                    break;
            }

            return res;
        };

        $scope.importDomainPrev = function () {
            $scope.importDomain.button = 'Next';

            if ($scope.importDomain.action === 'options') {
                $scope.importDomain.action = 'tables';
                $scope.importDomain.info = INFO_SELECT_TABLES;
            }
            else if ($scope.importDomain.action === 'tables' && $scope.importDomain.schemas.length > 0) {
                $scope.importDomain.action = 'schemas';
                $scope.importDomain.info = INFO_SELECT_SCHEMAS;
                $scope.importDomain.loadingOptions = LOADING_TABLES;
            }
            else {
                $scope.importDomain.action = 'connect';
                $scope.importDomain.info = INFO_CONNECT_TO_DB;
                $scope.importDomain.loadingOptions = LOADING_SCHEMAS;
            }
        };

        $scope.domainModelTitle = function () {
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

        function _fillCommonCachesOrTemplates(item) {
            return function (action) {
                if (item.cachesOrTemplates)
                    item.cachesOrTemplates.length = 0;
                else
                    item.cachesOrTemplates = [];

                if (action == IMPORT_DM_NEW_CACHE) {
                    item.cachesOrTemplates.push(DFLT_PARTITIONED_CACHE);
                    item.cachesOrTemplates.push(DFLT_REPLICATED_CACHE);
                }

                if (!$common.isEmptyArray($scope.caches)) {
                    if (item.cachesOrTemplates.length > 0)
                        item.cachesOrTemplates.push(null);

                    _.forEach($scope.caches, function (cache) {
                        item.cachesOrTemplates.push(cache);
                    });
                }

                if (!_.find(item.cachesOrTemplates, {value: item.cacheOrTemplate}))
                    item.cacheOrTemplate = item.cachesOrTemplates[0].value;
            }
        }

        // When landing on the page, get domain models and show them.
        $loading.start('loadingDomainModelsScreen');

        var _importCachesOrTemplates = [];

        $http.post('/api/v1/configuration/domains/list')
            .success(function (data) {
                $scope.spaces = data.spaces;
                $scope.clusters = _.map(data.clusters, function (cluster) {
                    return {
                        value: cluster._id,
                        label: cluster.name
                    };
                });
                $scope.caches = _mapCaches(data.caches);
                $scope.domains = data.domains;

                _.forEach($scope.clusters, function (cluster) {
                    $scope.ui.generatedCachesClusters.push(cluster.value);
                });

                if (!$common.isEmptyArray($scope.caches))
                    $scope.importActions.push({
                        label: 'Associate with existing cache',
                        shortLabel: 'Associate',
                        value: IMPORT_DM_ASSOCIATE_CACHE
                    });

                $scope.$watch('importCommon.action', _fillCommonCachesOrTemplates($scope.importCommon), true);

                $scope.importCommon.action = IMPORT_DM_NEW_CACHE;

                // Load page descriptor.
                $http.get('/models/domains.json')
                    .success(function (data) {
                        $scope.domainModel = data.domainModel;

                        $scope.ui.groups = data.domainModel;

                        if ($state.params.id)
                            $scope.createItem($state.params.id);
                        else
                            restoreSelection();

                        $timeout(function () {
                            $scope.$apply();
                        });

                        $scope.$watch('backupItem', function (val) {
                            if (val) {
                                var srcItem = $scope.selectedItem ? $scope.selectedItem : prepareNewItem();

                                $scope.ui.checkDirty(val, srcItem);

                                $scope.preview.general.xml = $generatorXml.domainModelGeneral(val).asString();
                                $scope.preview.general.java = $generatorJava.domainModelGeneral(val).asString();
                                $scope.preview.general.allDefaults = $common.isEmptyString($scope.preview.general.xml);

                                $scope.preview.query.xml = $generatorXml.domainModelQuery(val).asString();
                                $scope.preview.query.java = $generatorJava.domainModelQuery(val).asString();
                                $scope.preview.query.allDefaults = $common.isEmptyString($scope.preview.query.xml);

                                $scope.preview.store.xml = $generatorXml.domainStore(val).asString();
                                $scope.preview.store.java = $generatorJava.domainStore(val, false).asString();
                                $scope.preview.store.allDefaults = $common.isEmptyString($scope.preview.store.xml);
                            }
                        }, true);
                    })
                    .error(function (errMsg) {
                        $common.showError(errMsg);
                    });
            })
            .error(function (errMsg) {
                $common.showError(errMsg);
            })
            .finally(function () {
                $scope.ui.ready = true;
                $loading.finish('loadingDomainModelsScreen');
            });

        $scope.selectItem = function (item, backup) {
            function selectItem() {
                $table.tableReset();

                $scope.selectedItem = angular.copy(item);

                try {
                    if (item && item._id)
                        sessionStorage.lastSelectedDomain = angular.toJson(item._id);
                    else
                        sessionStorage.removeItem('lastSelectedDomain');
                }
                catch (ignored) {
                    // Ignore possible errors when read from storage.
                }

                _.forEach(previews, function (preview) {
                    preview.attractAttention = false;
                });

                if (backup)
                    $scope.backupItem = backup;
                else if (item)
                    $scope.backupItem = angular.copy(item);
                else
                    $scope.backupItem = undefined;

                if ($common.isDefined($scope.backupItem) && !$common.isDefined($scope.backupItem.queryMetadata))
                    $scope.backupItem.queryMetadata = 'Configuration';

                if ($common.isDefined($scope.selectedItem) && !$common.isDefined($scope.selectedItem.queryMetadata))
                    $scope.selectedItem.queryMetadata = 'Configuration';

                if ($common.getQueryVariable('new'))
                    $state.go('base.configuration.domains');
            }

            $common.confirmUnsavedChanges($scope.ui.isDirty(), selectItem);
        };

        function prepareNewItem(cacheId) {
            return {
                space: $scope.spaces[0]._id,
                caches: cacheId && _.find($scope.caches, {value: cacheId}) ? [cacheId] :
                    (!$common.isEmptyArray($scope.caches) ? [$scope.caches[0].value] : []),
                queryMetadata: 'Configuration'
            };
        }

        // Add new domain model.
        $scope.createItem = function (cacheId) {
            if ($scope.tableReset(true)) {
                $timeout(function () {
                    $common.ensureActivePanel($scope.ui, 'query');
                    $common.ensureActivePanel($scope.ui, 'general', 'keyType');
                });

                $scope.selectItem(undefined, prepareNewItem(cacheId));
            }
        };

        // Check domain model logical consistency.
        function validate(item) {
            if ($common.isEmptyString(item.keyType))
                return showPopoverMessage($scope.ui, 'general', 'keyType', 'Key type should not be empty');
            else if (!$common.isValidJavaClass('Key type', item.keyType, true, 'keyType'))
                return false;

            if ($common.isEmptyString(item.valueType))
                return showPopoverMessage($scope.ui, 'general', 'valueType', 'Value type should not be empty');
            else if (!$common.isValidJavaClass('Value type', item.valueType, false, 'valueType'))
                return false;

            var str = $common.domainForStoreConfigured(item);
            var qry = $common.domainForQueryConfigured(item);

            if (item.queryMetadata === 'Configuration' && qry) {
                if ($common.isEmptyArray(item.fields))
                    return showPopoverMessage($scope.ui, 'query', 'fields-legend', 'Query fields should not be empty');

                var indexes = item.indexes;

                if (indexes && indexes.length > 0) {
                    if (_.find(indexes, function (index, i) {
                            if ($common.isEmptyArray(index.fields))
                                return !showPopoverMessage($scope.ui, 'query', 'indexes' + i, 'Index fields are not specified');
                        }))
                        return false;
                }
            }

            if (str) {
                if ($common.isEmptyString(item.databaseSchema))
                    return showPopoverMessage($scope.ui, 'store', 'databaseSchema', 'Database schema should not be empty');

                if ($common.isEmptyString(item.databaseTable))
                    return showPopoverMessage($scope.ui, 'store', 'databaseTable', 'Database table should not be empty');

                if ($common.isEmptyArray(item.keyFields))
                    return showPopoverMessage($scope.ui, 'store', 'keyFields-add', 'Key fields are not specified');

                if ($common.isJavaBuiltInClass(item.keyType) && item.keyFields.length !== 1)
                    return showPopoverMessage($scope.ui, 'store', 'keyFields-add', 'Only one field should be specified in case when key type is a Java built-in type');

                if ($common.isEmptyArray(item.valueFields))
                    return showPopoverMessage($scope.ui, 'store', 'valueFields-add', 'Value fields are not specified');
            }
            else if (!qry && item.queryMetadata === 'Configuration') {
                return showPopoverMessage($scope.ui, 'query', 'query-title', 'SQL query domain model should be configured');
            }

            return true;
        }

        // Save domain models into database.
        function save(item) {
            var qry = $common.domainForQueryConfigured(item);
            var str = $common.domainForStoreConfigured(item);

            item.kind = 'query';

            if (qry && str)
                item.kind = 'both';
            else if (str)
                item.kind = 'store';

            $http.post('/api/v1/configuration/domains/save', item)
                .success(function (res) {
                    $scope.ui.markPristine();

                    var savedMeta = res.savedDomains[0];

                    var idx = _.findIndex($scope.domains, function (domain) {
                        return domain._id === savedMeta._id;
                    });

                    if (idx >= 0)
                        angular.extend($scope.domains[idx], savedMeta);
                    else
                        $scope.domains.push(savedMeta);

                    $scope.selectItem(savedMeta);

                    $common.showInfo('Domain model "' + item.valueType + '" saved.');

                    _checkShowValidPresentation();
                })
                .error(function (errMsg) {
                    $common.showError(errMsg);
                });
        }

        // Save domain model.
        $scope.saveItem = function () {
            if ($scope.tableReset(true)) {
                var item = $scope.backupItem;

                item.cacheStoreChanges = [];

                _.forEach(item.caches, function (cacheId) {
                    var cache = _.find($scope.caches, {value: cacheId}).cache;

                    var change = $common.autoCacheStoreConfiguration(cache, [item]);

                    if (change)
                        item.cacheStoreChanges.push({cacheId: cacheId, change: change});
                });

                if (validate(item))
                    save(item);
            }
        };

        function _domainNames() {
            return _.map($scope.domains, function (domain) {
                return domain.valueType;
            });
        }

        function _newNameIsValidJavaClass(newName) {
            return $common.isValidJavaClass('New name for value type', newName, false, 'copy-new-name');
        }

        // Save domain model with new name.
        $scope.cloneItem = function () {
            if ($scope.tableReset(true) && validate($scope.backupItem)) {
                $clone.confirm($scope.backupItem.valueType, _domainNames(), _newNameIsValidJavaClass).then(function (newName) {
                    var item = angular.copy($scope.backupItem);

                    delete item._id;
                    delete item.demo;
                    item.valueType = newName;

                    save(item);
                });
            }
        };

        // Remove domain model from db.
        $scope.removeItem = function () {
            $table.tableReset();

            var selectedItem = $scope.selectedItem;

            $confirm.confirm('Are you sure you want to remove domain model: "' + selectedItem.valueType + '"?')
                .then(function () {
                    var _id = selectedItem._id;

                    $http.post('/api/v1/configuration/domains/remove', {_id: _id})
                        .success(function () {
                            $common.showInfo('Domain model has been removed: ' + selectedItem.valueType);

                            var domains = $scope.domains;

                            var idx = _.findIndex(domains, function (domain) {
                                return domain._id === _id;
                            });

                            if (idx >= 0) {
                                domains.splice(idx, 1);

                                if (domains.length > 0)
                                    $scope.selectItem(domains[0]);
                                else
                                    $scope.selectItem(undefined, undefined);
                            }

                            _checkShowValidPresentation();
                        })
                        .error(function (errMsg) {
                            $common.showError(errMsg);
                        });
                });
        };

        function _checkShowValidPresentation() {
            if (!$scope.ui.showValid) {
                var validFilter = $filter('domainsValidation');

                $scope.ui.showValid = validFilter($scope.domains, false, true).length === 0;
            }
        }

        // Remove all domain models from db.
        $scope.removeAllItems = function () {
            $table.tableReset();

            $confirm.confirm('Are you sure you want to remove all domain models?')
                .then(function () {
                    $http.post('/api/v1/configuration/domains/remove/all')
                        .success(function () {
                            $common.showInfo('All domain models have been removed');

                            $scope.domains = [];
                            $scope.ui.markPristine();
                            $scope.selectItem(undefined, undefined);
                            $scope.ui.showValid = true;
                        })
                        .error(function (errMsg) {
                            $common.showError(errMsg);
                        });
                });
        };

        $scope.toggleValid = function () {
            $scope.ui.showValid = !$scope.ui.showValid;

            var validFilter = $filter('domainsValidation');

            var idx = -1;

            if ($common.isDefined($scope.selectedItem)) {
                idx = _.findIndex(validFilter($scope.domains, $scope.ui.showValid, true), function (domain) {
                    return domain._id === $scope.selectedItem._id;
                });
            }

            if (idx === -1)
                $scope.selectItem(undefined, undefined);
        };

        var pairFields = {
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

        $scope.tablePairValid = function (item, field, index) {
            var pairField = pairFields[field.model];

            var pairValue = $table.tablePairValue(field, index);

            if (pairField) {
                var model = item[field.model];

                if ($common.isDefined(model)) {
                    var idx = _.findIndex(model, function (pair) {
                        return pair[pairField.searchCol] === pairValue[pairField.valueCol];
                    });

                    // Found duplicate by key.
                    if (idx >= 0 && idx !== index)
                        return showPopoverMessage($scope.ui, 'query', $table.tableFieldId(index, pairField.idPrefix + pairField.id), 'Field with such ' + pairField.dupObjName + ' already exists!');
                }

                if (pairField.classValidation && !$common.isValidJavaClass(pairField.msg, pairValue.value, true, $table.tableFieldId(index, 'Value' + pairField.id)))
                    return $table.tableFocusInvalidField(index, 'Value' + pairField.id);
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

        $scope.tableDbFieldSaveVisible = function (field, index) {
            var dbFieldValue = tableDbFieldValue(field, index);

            return $common.isDefined(dbFieldValue.databaseFieldType) &&
                $common.isDefined(dbFieldValue.javaFieldType) &&
                !$common.isEmptyString(dbFieldValue.databaseFieldName) &&
                !$common.isEmptyString(dbFieldValue.javaFieldName);
        };

        var dbFieldTables = {
            keyFields: {msg: 'Key field', id: 'KeyField'},
            valueFields: {msg: 'Value field', id: 'ValueField'}
        };

        $scope.tableDbFieldSave = function (field, index, stopEdit) {
            var dbFieldTable = dbFieldTables[field.model];

            if (dbFieldTable) {
                var dbFieldValue = tableDbFieldValue(field, index);

                var item = $scope.backupItem;

                var model = item[field.model];

                if (!$common.isValidJavaIdentifier(dbFieldTable.msg + ' java name', dbFieldValue.javaFieldName, $table.tableFieldId(index, 'JavaFieldName' + dbFieldTable.id)))
                    return false;

                if ($common.isDefined(model)) {
                    var idx = _.findIndex(model, function (dbMeta) {
                        return dbMeta.databaseFieldName === dbFieldValue.databaseFieldName;
                    });

                    // Found duplicate.
                    if (idx >= 0 && index !== idx)
                        return showPopoverMessage($scope.ui, 'store', $table.tableFieldId(index, 'DatabaseFieldName' + dbFieldTable.id), 'Field with such database name already exists!');

                    idx = _.findIndex(model, function (dbMeta) {
                        return dbMeta.javaFieldName === dbFieldValue.javaFieldName;
                    });

                    // Found duplicate.
                    if (idx >= 0 && index !== idx)
                        return showPopoverMessage($scope.ui, 'store', $table.tableFieldId(index, 'JavaFieldName' + dbFieldTable.id), 'Field with such java name already exists!');

                    if (index < 0) {
                        model.push(dbFieldValue);
                    }
                    else {
                        var dbField = model[index];

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
                        $table.tableNewItem(field);
                    else if (index < model.length - 1)
                        $table.tableStartEdit(item, field, index + 1);
                    else
                        $table.tableNewItem(field);
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

        $scope.tableIndexSaveVisible = function (field, index) {
            return !$common.isEmptyString(tableIndexName(field, index)) && $common.isDefined(tableIndexType(field, index));
        };

        $scope.tableIndexSave = function (field, curIdx, stopEdit) {
            var indexName = tableIndexName(field, curIdx);
            var indexType = tableIndexType(field, curIdx);

            var item = $scope.backupItem;

            var indexes = item.indexes;

            if ($common.isDefined(indexes)) {
                var idx = _.findIndex(indexes, function (index) {
                    return index.name === indexName;
                });

                // Found duplicate.
                if (idx >= 0 && idx !== curIdx)
                    return showPopoverMessage($scope.ui, 'query', $table.tableFieldId(curIdx, 'IndexName'), 'Index with such name already exists!');
            }

            $table.tableReset();

            if (curIdx < 0) {
                var newIndex = {name: indexName, indexType: indexType};

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
                    var index = item.indexes[curIdx];

                    if (index.fields && index.fields.length > 0)
                        $scope.tableIndexItemStartEdit(field, curIdx, 0);
                    else
                        $scope.tableIndexNewItem(field, curIdx);
                }
            }

            return true;
        };

        $scope.tableIndexNewItem = function (field, indexIdx) {
            if ($scope.tableReset(true)) {
                var index = $scope.backupItem.indexes[indexIdx];

                $table.tableState(field, -1, 'table-index-fields');
                $table.tableFocusInvalidField(-1, 'FieldName' + (index.indexType === 'SORTED' ? 'S' : '') + indexIdx);

                field.newFieldName = null;
                field.newDirection = true;
                field.indexIdx = indexIdx;
            }
        };

        $scope.tableIndexNewItemActive = function (field, itemIndex) {
            var indexes = $scope.backupItem.indexes;

            if (indexes) {
                var index = indexes[itemIndex];

                if (index)
                    return $table.tableNewItemActive({model: 'table-index-fields'}) && field.indexIdx === itemIndex;
            }

            return false;
        };

        $scope.tableIndexItemEditing = function (field, itemIndex, curIdx) {
            var indexes = $scope.backupItem.indexes;

            if (indexes) {
                var index = indexes[itemIndex];

                if (index)
                    return $table.tableEditing({model: 'table-index-fields'}, curIdx) && field.indexIdx === itemIndex;
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

        $scope.tableIndexItemStartEdit = function (field, indexIdx, curIdx) {
            if ($scope.tableReset(true)) {
                var index = $scope.backupItem.indexes[indexIdx];

                $table.tableState(field, curIdx, 'table-index-fields');

                var indexItem = index.fields[curIdx];

                field.curFieldName = indexItem.name;
                field.curDirection = indexItem.direction;
                field.indexIdx = indexIdx;

                $focus('curFieldName' + (index.indexType === 'SORTED' ? 'S' : '') + field.indexIdx + '-' + curIdx);
            }
        };

        $scope.tableIndexItemSaveVisible = function (field, index) {
            return !$common.isEmptyString(tableIndexItemValue(field, index).name);
        };

        $scope.tableIndexItemSave = function (field, indexIdx, curIdx, stopEdit) {
            var indexItemValue = tableIndexItemValue(field, curIdx);

            var index = $scope.backupItem.indexes[indexIdx];

            var fields = index.fields;

            if ($common.isDefined(fields)) {
                var idx = _.findIndex(fields, function (field) {
                    return field.name === indexItemValue.name;
                });

                // Found duplicate.
                if (idx >= 0 && idx !== curIdx)
                    return showPopoverMessage($scope.ui, 'query', $table.tableFieldId(curIdx, 'FieldName' + (index.indexType === 'SORTED' ? 'S' : '') + indexIdx + (curIdx >= 0 ? '-' : '')), 'Field with such name already exists in index!');
            }

            $table.tableReset();

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

        $scope.tableRemoveIndexItem = function (index, curIdx) {
            $table.tableReset();

            index.fields.splice(curIdx, 1);
        };

        $scope.resetItem = function (group) {
            var resetTo = $scope.selectedItem;

            if (!$common.isDefined(resetTo))
                resetTo = prepareNewItem();

            $common.resetItem($scope.backupItem, resetTo, $scope.domain, group);
        };

        $scope.resetAll = function () {
            $table.tableReset();

            $confirm.confirm('Are you sure you want to undo all changes for current domain model?')
                .then(function () {
                    $scope.backupItem = $scope.selectedItem ? angular.copy($scope.selectedItem) : prepareNewItem();
                    $scope.ui.markPristine();
                });
        };
    }]
);
