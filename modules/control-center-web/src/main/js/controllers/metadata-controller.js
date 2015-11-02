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

// Controller for Metadata screen.
consoleModule.controller('metadataController', [
    '$scope', '$controller', '$filter', '$http', '$modal', '$common', '$timeout', '$focus', '$confirm', '$confirmBatch', '$message', '$clone', '$table', '$preview', '$loading', '$unsavedChangesGuard',
    function ($scope, $controller, $filter, $http, $modal, $common, $timeout, $focus, $confirm, $confirmBatch, $message, $clone, $table, $preview, $loading, $unsavedChangesGuard) {
            $unsavedChangesGuard.install($scope);

            // Initialize the super class and extend it.
            angular.extend(this, $controller('save-remove', {$scope: $scope}));

            // Initialize the super class and extend it.
            angular.extend(this, $controller('agent-download', {$scope: $scope}));

            $scope.ui = $common.formUI();

            $scope.showMoreInfo = $message.message;

            $scope.agentGoal = 'load metadata from database schema';
            $scope.agentTestDriveOption = '--test-drive-metadata';

            $scope.joinTip = $common.joinTip;
            $scope.getModel = $common.getModel;
            $scope.javaBuildInClasses = $common.javaBuildInClasses;
            $scope.compactJavaName = $common.compactJavaName;
            $scope.saveBtnTipText = $common.saveBtnTipText;
            $scope.panelExpanded = $common.panelExpanded;

            $scope.tableVisibleRow = $table.tableVisibleRow;
            $scope.tableReset = $table.tableReset;
            $scope.tableNewItem = $table.tableNewItem;
            $scope.tableNewItemActive = $table.tableNewItemActive;
            $scope.tableEditing = $table.tableEditing;
            $scope.tableStartEdit = $table.tableStartEdit;
            $scope.tableRemove = function (item, field, index) {
                $table.tableRemove(item, field, index);
            };

            $scope.tableSimpleSave = $table.tableSimpleSave;
            $scope.tableSimpleSaveVisible = $table.tableSimpleSaveVisible;
            $scope.tableSimpleUp = $table.tableSimpleUp;
            $scope.tableSimpleDown = $table.tableSimpleDown;
            $scope.tableSimpleDownVisible = $table.tableSimpleDownVisible;

            $scope.tablePairStartEdit = $table.tablePairStartEdit;
            $scope.tablePairSave = $table.tablePairSave;
            $scope.tablePairSaveVisible = $table.tablePairSaveVisible;

            var INFO_CONNECT_TO_DB = 'Configure connection to database';
            var INFO_SELECT_SCHEMAS = 'Select schemas to load tables from';
            var INFO_SELECT_TABLES = 'Select tables to import as cache type metadata';
            var LOADING_JDBC_DRIVERS = {text: 'Loading JDBC drivers...'};
            var LOADING_SCHEMAS = {text: 'Loading schemas...'};
            var LOADING_TABLES = {text: 'Loading tables...'};
            var LOADING_METADATA = {text: 'Loading metadata...'};

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

            var presets = [
                {
                    db: 'oracle',
                    jdbcDriverClass: 'oracle.jdbc.OracleDriver',
                    jdbcUrl: 'jdbc:oracle:thin:@[host]:[port]:[database]',
                    user: 'system'
                },
                {
                    db: 'db2',
                    jdbcDriverClass: 'com.ibm.db2.jcc.DB2Driver',
                    jdbcUrl: 'jdbc:db2://[host]:[port]/[database]',
                    user: 'db2admin'
                },
                {
                    db: 'mssql',
                    jdbcDriverClass: 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
                    jdbcUrl: 'jdbc:sqlserver://[host]:[port][;databaseName=database]',
                    user: 'sa'
                },
                {
                    db: 'postgre',
                    jdbcDriverClass: 'org.postgresql.Driver',
                    jdbcUrl: 'jdbc:postgresql://[host]:[port]/[database]',
                    user: 'sa'
                },
                {
                    db: 'mysql',
                    jdbcDriverClass: 'com.mysql.jdbc.Driver',
                    jdbcUrl: 'jdbc:mysql://[host]:[port]/[database]',
                    user: 'root'
                },
                {
                    db: 'h2',
                    jdbcDriverClass: 'org.h2.Driver',
                    jdbcUrl: 'jdbc:h2:tcp://[host]/[database]',
                    user: 'sa'
                }
            ];

            $scope.preset = {
                db: 'unknown',
                jdbcDriverClass: '',
                jdbcDriverJar: '',
                jdbcUrl: 'jdbc:[database]',
                user: 'sa',
                tablesOnly: true
            };

            $scope.ui.showValid = true;

            var jdbcDrivers = [];

            function _findPreset(jdbcDriverJar) {
                var jdbcDriverClass = '';

                var idx = _.findIndex(jdbcDrivers, function (jdbcDriver) {
                    return  jdbcDriver.jdbcDriverJar == jdbcDriverJar;
                });

                if (idx >= 0) {
                    jdbcDriverClass = jdbcDrivers[idx].jdbcDriverClass;

                    idx = _.findIndex(presets, function (preset) {
                        return preset.jdbcDriverClass == jdbcDriverClass;
                    });

                    if (idx >= 0)
                        return presets[idx];
                }

                return {
                    db: 'unknown',
                    jdbcDriverClass: jdbcDriverClass,
                    jdbcDriverJar: '',
                    jdbcUrl: 'jdbc:[database]',
                    user: 'sa'
                }
            }

            $scope.$watch('preset.jdbcDriverJar', function (jdbcDriverJar) {
                if (jdbcDriverJar) {
                    var newPreset = _findPreset(jdbcDriverJar);

                    $scope.preset.db = newPreset.db;
                    $scope.preset.jdbcDriverClass = newPreset.jdbcDriverClass;
                    $scope.preset.jdbcUrl = newPreset.jdbcUrl;
                    $scope.preset.user = newPreset.user;
                }
            }, true);

            $scope.supportedJdbcTypes = $common.mkOptions($common.SUPPORTED_JDBC_TYPES);

            $scope.supportedJavaTypes = $common.mkOptions($common.javaBuildInClasses);

            $scope.sortDirections = [
                {value: true, label: 'ASC'},
                {value: false, label: 'DESC'}
            ];

            $scope.panels = {activePanels: [0, 1]};

            $scope.metadatas = [];

            $scope.isJavaBuildInClass = function () {
                var item = $scope.backupItem;

                if (item && item.keyType)
                    return $common.isJavaBuildInClass(item.keyType);

                return false;
            };

            $scope.selectAllSchemas = function () {
                var allSelected = $scope.loadMeta.allSchemasSelected;

                _.forEach($scope.loadMeta.displayedSchemas, function (schema) {
                    schema.use = allSelected;
                });
            };

            $scope.selectSchema = function () {
                if ($common.isDefined($scope.loadMeta) && $common.isDefined($scope.loadMeta.displayedSchemas))
                    $scope.loadMeta.allSchemasSelected = $scope.loadMeta.displayedSchemas.length > 0
                        && _.every($scope.loadMeta.displayedSchemas, 'use', true);
            };

            $scope.selectAllTables = function () {
                var allSelected = $scope.loadMeta.allTablesSelected;

                _.forEach($scope.loadMeta.displayedTables, function (table) {
                    table.use = allSelected;
                });
            };

            $scope.selectTable = function () {
                if ($common.isDefined($scope.loadMeta) && $common.isDefined($scope.loadMeta.displayedTables))
                    $scope.loadMeta.allTablesSelected = $scope.loadMeta.displayedTables.length > 0
                        && _.every($scope.loadMeta.displayedTables, 'use', true);
            };

            $scope.$watch('loadMeta.displayedSchemas', $scope.selectSchema);

            $scope.$watch('loadMeta.displayedTables', $scope.selectTable);

            // Pre-fetch modal dialogs.
            var loadMetaModal = $modal({scope: $scope, templateUrl: 'metadata/metadata-load', show: false});

            var hideLoadMetadata = loadMetaModal.hide;

            loadMetaModal.hide = function () {
                $scope.finishAgentListening();

                hideLoadMetadata();
            };

            // Show load metadata modal.
            $scope.showLoadMetadataModal = function () {
                $scope.loadMeta = {
                    action: 'connect',
                    schemas: [],
                    allSchemasSelected: false,
                    tables: [],
                    allTablesSelected: false,
                    button: 'Next',
                    info: ''
                };

                $scope.loadMeta.action = 'drivers';
                $scope.loadMeta.loadingOptions = LOADING_JDBC_DRIVERS;

                $scope.awaitAgent(function (result, onSuccess, onException) {
                    loadMetaModal.show();

                    $loading.start('loadingMetadataFromDb');

                    // Get available JDBC drivers via agent.
                    if ($scope.loadMeta.action == 'drivers') {
                        $http.post('/agent/drivers')
                            .success(function (drivers) {
                                onSuccess();

                                if (drivers && drivers.length > 0) {
                                    $scope.jdbcDriverJars = _.map(drivers, function (driver) {
                                        return {value: driver.jdbcDriverJar, label: driver.jdbcDriverJar};
                                    });

                                    jdbcDrivers = drivers;

                                    $scope.preset.jdbcDriverJar = drivers[0].jdbcDriverJar;

                                    function openLoadMetadataModal() {
                                        loadMetaModal.$promise.then(function () {
                                            $scope.loadMeta.action = 'connect';
                                            $scope.loadMeta.tables = [];
                                            $scope.loadMeta.loadingOptions = LOADING_SCHEMAS;

                                            $focus('jdbcUrl');
                                        });
                                    }

                                    $common.confirmUnsavedChanges($scope.ui.isDirty(), openLoadMetadataModal);
                                }
                                else {
                                    $common.showError('JDBC drivers not found!');

                                    loadMetaModal.hide();
                                }
                            })
                            .error(function (errMsg, status) {
                                onException(errMsg, status);
                            })
                            .finally(function () {
                                $scope.loadMeta.info = INFO_CONNECT_TO_DB;
                                $loading.finish('loadingMetadataFromDb');
                            });
                    }
                });
            };

            function _loadSchemas() {
                $loading.start('loadingMetadataFromDb');

                var preset = angular.copy($scope.preset);

                if (preset.jdbcUrl == 'jdbc:h2:mem:test-drive-db') {
                    preset.user = 'sa';
                    preset.password = '';
                }

                $http.post('/agent/schemas', preset)
                    .success(function (schemas) {
                        $scope.loadMeta.schemas = _.map(schemas, function (schema) { return {use: false, name: schema}});
                        $scope.loadMeta.action = 'schemas';
                    })
                    .error(function (errMsg) {
                        $common.showError(errMsg);
                    })
                    .finally(function() {
                        $scope.loadMeta.info = INFO_SELECT_SCHEMAS;
                        $scope.loadMeta.loadingOptions = LOADING_TABLES;

                        $loading.finish('loadingMetadataFromDb');
                    });
            }

            function _loadMetadata() {
                $loading.start('loadingMetadataFromDb');

                $scope.loadMeta.allTablesSelected = false;
                $scope.preset.schemas = [];

                _.forEach($scope.loadMeta.schemas, function (schema) {
                    if (schema.use)
                        $scope.preset.schemas.push(schema.name);
                });

                $http.post('/agent/metadata', $scope.preset)
                    .success(function (tables) {
                        $scope.loadMeta.tables = tables;
                        $scope.loadMeta.action = 'tables';
                        $scope.loadMeta.button = 'Save';

                        _.forEach(tables, function (tbl) {
                            tbl.use = $common.isDefined(_.find(tbl.cols, function (col) {
                                return col.key;
                            }));
                        })
                    })
                    .error(function (errMsg) {
                        $common.showError(errMsg);
                    })
                    .finally(function() {
                        $scope.loadMeta.info = INFO_SELECT_TABLES;
                        $scope.loadMeta.loadingOptions = LOADING_METADATA;

                        $loading.finish('loadingMetadataFromDb');
                    });
            }

            function toJavaClassName(name) {
                var len = name.length;

                var buf = '';

                var capitalizeNext = true;

                for (var i = 0; i < len; i++) {
                    var ch = name.charAt(i);

                    if (ch == ' ' ||  ch == '_')
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

            $scope.ui.packageName = $scope.user.email.replace('@', '.').split('.').reverse().join('.') + '.model';

            function _saveBatch(batch) {
                if (batch && batch.length > 0) {
                    $loading.start('loadingMetadataFromDb');

                    $http.post('metadata/save/batch', batch)
                        .success(function (savedBatch) {
                            var lastItem = undefined;
                            var newItems = [];

                            _.forEach(savedBatch, function (savedItem) {
                                var idx = _.findIndex($scope.metadatas, function (meta) {
                                    return meta._id == savedItem._id;
                                });

                                if (idx >= 0)
                                    $scope.metadatas[idx] = savedItem;
                                else
                                    newItems.push(savedItem);

                                lastItem = savedItem;
                            });

                            _.forEach(newItems, function (item) {
                                $scope.metadatas.push(item);
                            });

                            if (!lastItem && $scope.metadatas.length > 0)
                                lastItem = $scope.metadatas[0];

                            $scope.selectItem(lastItem);

                            $common.showInfo('Cache type metadata loaded from database.');

                            $scope.panels.activePanels = [0, 1, 2];

                            $scope.ui.showValid = true;
                        })
                        .error(function (errMsg) {
                            $common.showError(errMsg);
                        })
                        .finally(function() {
                            $loading.finish('loadingMetadataFromDb');

                            loadMetaModal.hide();
                        });
                }
                else
                    loadMetaModal.hide();
            }

            function _saveMetadata() {
                if ($common.isEmptyString($scope.ui.packageName))
                    return $common.showPopoverMessage(undefined, undefined, 'metadataLoadPackage',
                        'Package should be not empty');

                if (!$common.isValidJavaClass('Package', $scope.ui.packageName, false, 'metadataLoadPackage', true))
                    return false;

                $scope.preset.space = $scope.spaces[0];

                $http.post('presets/save', $scope.preset)
                    .error(function (errMsg) {
                        $common.showError(errMsg);
                    });

                var batch = [];
                var tables = [];
                var dupCnt = 0;

                var containKey = true;

                _.forEach($scope.loadMeta.tables, function (table) {
                    if (table.use) {
                        var qryFields = [];
                        var indexes = [];
                        var keyFields = [];
                        var valFields = [];

                        var tableName = table.tbl;

                        var dup = tables.indexOf(tableName) >= 0;

                        if (dup)
                            dupCnt++;

                        var valType = $scope.ui.packageName + '.' + toJavaClassName(tableName);

                        function queryField(name, jdbcType) {
                            return {name: toJavaName(name), className: jdbcType.javaType}
                        }

                        function dbField(name, jdbcType) {
                            return {databaseName: name, databaseType: jdbcType.dbName,
                                javaName: toJavaName(name), javaType: jdbcType.javaType}
                        }

                        var _containKey = false;

                        _.forEach(table.cols, function (col) {
                            var colName = col.name;
                            var jdbcType = $common.findJdbcType(col.type);

                            qryFields.push(queryField(colName, jdbcType));

                            if (col.key) {
                                keyFields.push(dbField(colName, jdbcType));

                                _containKey = true;
                            }
                            else
                                valFields.push(dbField(colName, jdbcType));
                        });

                        containKey &= _containKey;

                        var idxs = table.idxs;

                        if (table.idxs) {
                            var tblIndexes = Object.keys(idxs);

                            _.forEach(tblIndexes, function (indexName) {
                                var index = idxs[indexName];

                                var fields = Object.keys(index);

                                indexes.push(
                                    {name: indexName, type: 'SORTED', fields: _.map(fields, function (fieldName) {
                                        return {
                                            name: toJavaName(fieldName),
                                            direction: !index[fieldName]
                                        };
                                    })});
                            });
                        }

                        var metaFound = _.find($scope.metadatas, function (meta) {
                            return meta.valueType == valType;
                        });

                        var meta = {
                            confirm: false,
                            skip: false,
                            space: $scope.spaces[0],
                            caches: []
                        };

                        if ($common.isDefined(metaFound)) {
                            meta._id = metaFound._id;
                            meta.caches = metaFound.caches;
                            meta.confirm = true;
                        }

                        var dupSfx = (dup ? '_' + dupCnt : '');

                        meta.keyType = valType + 'Key' + dupSfx;
                        meta.valueType = valType + dupSfx;
                        meta.databaseSchema = table.schema;
                        meta.databaseTable = tableName;
                        meta.fields = qryFields;
                        meta.indexes = indexes;
                        meta.keyFields = keyFields;
                        meta.valueFields = valFields;

                        batch.push(meta);
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
                        'Metadata with name &quot;' + meta.databaseTable + '&quot; already exist.<br/><br/>' +
                        'Are you sure you want to overwrite it?' +
                        '</span>';
                }

                var itemsToConfirm = _.filter(batch, function (item) { return item.confirm; });

                function checkOverwrite() {
                    if (itemsToConfirm.length > 0)
                        $confirmBatch.confirm(overwriteMessage, itemsToConfirm)
                            .then(function () {
                                _saveBatch(_.filter(batch, function (item) {
                                    return !item.skip
                                }));
                            }, function () {
                                $common.showError('Cache type metadata loading interrupted by user.');
                            });
                    else
                        _saveBatch(batch);
                }

                if (containKey)
                    checkOverwrite();
                else
                    $confirm.confirm('Some tables have no primary key.<br/>' +
                        'You will need to configure key type and key fields for such tables after load complete.')
                        .then(function () { checkOverwrite(); })
            }

            $scope.loadMetadataNext = function () {
                if ($scope.loadMeta.action == 'connect')
                    _loadSchemas();
                else if ($scope.loadMeta.action == 'schemas')
                    _loadMetadata();
                else if ($scope.loadMeta.action == 'tables' && $scope.nextAvailable())
                    _saveMetadata();
            };

            $scope.nextTooltipText = function () {
                if ($scope.loadMeta.action == 'tables' && !$scope.nextAvailable())
                    return 'Select tables to continue';

                return undefined;
            };

            $scope.nextAvailable = function () {
                return $scope.loadMeta.action != 'tables' || $('#metadataTableData').find(':checked').length > 0;
            };

            $scope.loadMetadataPrev = function () {
                if  ($scope.loadMeta.action == 'tables') {
                    $scope.loadMeta.action = 'schemas';
                    $scope.loadMeta.button = 'Next';
                    $scope.loadMeta.info = INFO_SELECT_SCHEMAS;
                    $scope.loadMeta.loadingOptions = LOADING_TABLES;
                }
                else if  ($scope.loadMeta.action == 'schemas') {
                    $scope.loadMeta.action = 'connect';
                    $scope.loadMeta.info = INFO_CONNECT_TO_DB;
                    $scope.loadMeta.loadingOptions = LOADING_SCHEMAS;
                }
            };

            $scope.metadataTitle = function () {
                return $scope.ui.showValid ? 'Types metadata:' : 'Type metadata without key fields:';
            };

            function selectFirstItem() {
                if ($scope.metadatas.length > 0)
                    $scope.selectItem($scope.metadatas[0]);
            }

            // When landing on the page, get metadatas and show them.
            $loading.start('loadingMetadataScreen');

            $http.post('metadata/list')
                .success(function (data) {
                    $scope.spaces = data.spaces;
                    $scope.caches = data.caches;
                    $scope.metadatas = data.metadatas;

                    // Load page descriptor.
                    $http.get('/models/metadata.json')
                        .success(function (data) {
                            $scope.screenTip = data.screenTip;
                            $scope.moreInfo = data.moreInfo;
                            $scope.metadata = data.metadata;
                            $scope.metadataDb = data.metadataDb;

                            $scope.ui.groups = data.metadata;

                            if ($common.getQueryVariable('new'))
                                $scope.createItem($common.getQueryVariable('id'));
                            else {
                                var lastSelectedMetadata = angular.fromJson(sessionStorage.lastSelectedMetadata);

                                if (lastSelectedMetadata) {
                                    var idx = _.findIndex($scope.metadatas, function (metadata) {
                                        return metadata._id == lastSelectedMetadata;
                                    });

                                    if (idx >= 0)
                                        $scope.selectItem($scope.metadatas[idx]);
                                    else {
                                        sessionStorage.removeItem('lastSelectedMetadata');

                                        selectFirstItem();
                                    }
                                }
                                else
                                    selectFirstItem();
                            }

                            $timeout(function () {
                                $scope.$apply();
                            });

                            $scope.$watch('backupItem', function (val) {
                                if (val) {
                                    var srcItem = $scope.selectedItem ? $scope.selectedItem : prepareNewItem();

                                    $scope.ui.checkDirty(val, srcItem);

                                    $scope.preview.general.xml = $generatorXml.metadataGeneral(val).asString();
                                    $scope.preview.general.java = $generatorJava.metadataGeneral(val).asString();
                                    $scope.preview.general.allDefaults = $common.isEmptyString($scope.preview.general.xml);

                                    $scope.preview.query.xml = $generatorXml.metadataQuery(val).asString();
                                    $scope.preview.query.java = $generatorJava.metadataQuery(val).asString();
                                    $scope.preview.query.allDefaults = $common.isEmptyString($scope.preview.query.xml);

                                    $scope.preview.store.xml = $generatorXml.metadataStore(val).asString();
                                    $scope.preview.store.java = $generatorJava.metadataStore(val, false).asString();
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
                .finally(function() {
                    $scope.ui.ready = true;
                    $loading.finish('loadingMetadataScreen');
                });

            $http.post('presets/list')
                .success(function (data) {
                    _.forEach(data.presets, function (restoredPreset) {
                        var preset = _.find(presets, function (dfltPreset) {
                            return dfltPreset.jdbcDriverClass == restoredPreset.jdbcDriverClass;
                        });

                        if (preset) {
                            preset.jdbcUrl = restoredPreset.jdbcUrl;
                            preset.user = restoredPreset.user;
                        }
                    });
                })
                .error(function (errMsg) {
                    $common.showError(errMsg);
                });

            $scope.selectItem = function (item, backup) {
                function selectItem() {
                    $table.tableReset();

                    $scope.selectedItem = angular.copy(item);

                    try {
                        if (item && item._id)
                            sessionStorage.lastSelectedMetadata = angular.toJson(item._id);
                        else
                            sessionStorage.removeItem('lastSelectedMetadata');
                    }
                    catch (error) { }

                    _.forEach(previews, function(preview) {
                        preview.attractAttention = false;
                    });

                    if (backup)
                        $scope.backupItem = backup;
                    else if (item)
                        $scope.backupItem = angular.copy(item);
                    else
                        $scope.backupItem = undefined;
                }

                $common.confirmUnsavedChanges($scope.ui.isDirty(), selectItem);

                $scope.ui.formTitle = $common.isDefined($scope.backupItem) && $scope.backupItem._id
                    ? 'Selected metadata: ' + $scope.backupItem.valueType
                    : 'New metadata';
            };

            function prepareNewItem(cacheId) {
                return {
                    space: $scope.spaces[0]._id,
                    caches: cacheId && _.find($scope.caches, {value: cacheId}) ? [cacheId] : []
                };
            }

            // Add new metadata.
            $scope.createItem = function (cacheId) {
                $table.tableReset();

                $timeout(function () {
                    $common.ensureActivePanel($scope.panels, 'query');
                    $common.ensureActivePanel($scope.panels, 'general', 'keyType');
                });

                $scope.selectItem(undefined, prepareNewItem(cacheId));
            };

            // Check metadata logical consistency.
            function validate(item) {
                if ($common.isEmptyString(item.keyType))
                    return showPopoverMessage($scope.panels, 'general', 'keyType', 'Key type should not be empty');
                else if (!$common.isValidJavaClass('Key type', item.keyType, true, 'keyType'))
                    return false;

                if ($common.isEmptyString(item.valueType))
                    return showPopoverMessage($scope.panels, 'general', 'valueType', 'Value type should not be empty');
                else if (!$common.isValidJavaClass('Value type', item.valueType, false, 'valueType'))
                    return false;

                var qry = $common.metadataForQueryConfigured(item);

                if (qry) {
                    var indexes = item.indexes;

                    if (indexes && indexes.length > 0) {
                        _.forEach(indexes, function(index) {
                            if ($common.isEmptyArray(index.fields))
                                return showPopoverMessage($scope.panels, 'query', 'indexes' + i, 'Group fields are not specified');
                        });
                    }
                }

                var str = $common.metadataForStoreConfigured(item);

                if (str) {
                    if ($common.isEmptyString(item.databaseSchema))
                        return showPopoverMessage($scope.panels, 'store', 'databaseSchema', 'Database schema should not be empty');

                    if ($common.isEmptyString(item.databaseTable))
                        return showPopoverMessage($scope.panels, 'store', 'databaseTable', 'Database table should not be empty');

                    if ($common.isEmptyArray(item.keyFields) && !$common.isJavaBuildInClass(item.keyType))
                        return showPopoverMessage($scope.panels, 'store', 'keyFields-add', 'Key fields are not specified');

                    if ($common.isEmptyArray(item.valueFields))
                        return showPopoverMessage($scope.panels, 'store', 'valueFields-add', 'Value fields are not specified');
                }
                else if (!qry) {
                    return showPopoverMessage($scope.panels, 'query', 'query-title', 'SQL query metadata should be configured');
                }

                return true;
            }

            // Save cache type metadata into database.
            function save(item) {
                var qry = $common.metadataForQueryConfigured(item);
                var str = $common.metadataForStoreConfigured(item);

                item.kind = 'query';

                if (qry && str)
                    item.kind = 'both';
                else if (str)
                    item.kind = 'store';

                $http.post('metadata/save', item)
                    .success(function (res) {
                        $scope.ui.markPristine();

                        var savedMeta = res[0];

                        var idx = _.findIndex($scope.metadatas, function (metadata) {
                            return metadata._id == savedMeta._id;
                        });

                        if (idx >= 0)
                            angular.extend($scope.metadatas[idx], savedMeta);
                        else
                            $scope.metadatas.push(savedMeta);

                        $scope.selectItem(savedMeta);

                        $common.showInfo('Cache type metadata"' + item.valueType + '" saved.');
                    })
                    .error(function (errMsg) {
                        $common.showError(errMsg);
                    });
            }

            // Save cache type metadata.
            $scope.saveItem = function () {
                $table.tableReset();

                var item = $scope.backupItem;

                //if (validate(item))
                save(item);
            };

            // Save cache type metadata with new name.
            $scope.cloneItem = function () {
                $table.tableReset();

                if (validate($scope.backupItem))
                    $clone.confirm($scope.backupItem.valueType).then(function (newName) {
                        var item = angular.copy($scope.backupItem);

                        item._id = undefined;
                        item.valueType = newName;

                        save(item);
                    });
            };

            // Remove metadata from db.
            $scope.removeItem = function () {
                $table.tableReset();

                var selectedItem = $scope.selectedItem;

                $confirm.confirm('Are you sure you want to remove cache type metadata: "' + selectedItem.valueType + '"?')
                    .then(function () {
                            var _id = selectedItem._id;

                            $http.post('metadata/remove', {_id: _id})
                                .success(function () {
                                    $common.showInfo('Cache type metadata has been removed: ' + selectedItem.valueType);

                                    var metadatas = $scope.metadatas;

                                    var idx = _.findIndex(metadatas, function (metadata) {
                                        return metadata._id == _id;
                                    });

                                    if (idx >= 0) {
                                        metadatas.splice(idx, 1);

                                        if (metadatas.length > 0)
                                            $scope.selectItem(metadatas[0]);
                                        else
                                            $scope.selectItem(undefined, undefined);
                                    }

                                    if (!$scope.ui.showValid) {
                                        var validFilter = $filter('metadatasValidation');

                                        $scope.ui.showValid = validFilter($scope.metadatas, false, true).length == 0;
                                    }
                                })
                                .error(function (errMsg) {
                                    $common.showError(errMsg);
                                });
                    });
            };

            // Remove all metadata from db.
            $scope.removeAllItems = function () {
                $table.tableReset();

                $confirm.confirm('Are you sure you want to remove all metadata?')
                    .then(function () {
                            $http.post('metadata/remove/all')
                                .success(function () {
                                    $common.showInfo('All metadata have been removed');

                                    $scope.metadatas = [];

                                    $scope.selectItem(undefined, undefined);

                                    $scope.ui.showValid = true;
                                })
                                .error(function (errMsg) {
                                    $common.showError(errMsg);
                                });
                    });
            };

            $scope.tableSimpleValid = function (item, field, name, index) {
                var model = item[field.model];

                if ($common.isDefined(model)) {
                    var idx = _.indexOf(model, name);

                    // Found duplicate.
                    if (idx >= 0 && idx != index)
                        return showPopoverMessage(null, null, $table.tableFieldId(index, 'TextField'), 'Field with such name already exists!');
                }

                return true;
            };

            var pairFields = {
                fields: {msg: 'Query field class', id: 'QryField', idPrefix: 'Key', searchCol: 'name', valueCol: 'key', classValidation: true, dupObjName: 'name'},
                aliases: {id: 'Alias', idPrefix: 'Value', searchCol: 'alias', valueCol: 'value', dupObjName: 'alias'}
            };

            $scope.tablePairValid = function (item, field, index) {
                var pairField = pairFields[field.model];

                var pairValue = $table.tablePairValue(field, index);

                if (pairField) {
                    var model = item[field.model];

                    if ($common.isDefined(model)) {
                        var idx = _.findIndex(model, function (pair) {
                            return pair[pairField.searchCol] == pairValue[pairField.valueCol];
                        });

                        // Found duplicate by key.
                        if (idx >= 0 && idx != index)
                            return showPopoverMessage(null, null, $table.tableFieldId(index, pairField.idPrefix + pairField.id), 'Field with such ' + pairField.dupObjName + ' already exists!');
                    }

                    if (pairField.classValidation && !$common.isValidJavaClass(pairField.msg, pairValue.value, true, $table.tableFieldId(index, 'Value' + pairField.id)))
                        return $table.tableFocusInvalidField(index, 'Value' + pairField.id);
                }

                return true;
            };

            function tableDbFieldValue(field, index) {
                return index < 0
                    ? {databaseName: field.newDatabaseName, databaseType: field.newDatabaseType, javaName: field.newJavaName, javaType: field.newJavaType}
                    : {databaseName: field.curDatabaseName, databaseType: field.curDatabaseType, javaName: field.curJavaName, javaType: field.curJavaType}
            }

            $scope.tableDbFieldSaveVisible = function (field, index) {
                var dbFieldValue = tableDbFieldValue(field, index);

                return !$common.isEmptyString(dbFieldValue.databaseName) && $common.isDefined(dbFieldValue.databaseType) &&
                    !$common.isEmptyString(dbFieldValue.javaName) && $common.isDefined(dbFieldValue.javaType);
            };

            var dbFieldTables = {
                keyFields: {msg: 'Key field', id: 'KeyField'},
                valueFields: {msg: 'Value field', id: 'ValueField'}
            };

            $scope.tableDbFieldSave = function (field, index) {
                var dbFieldTable = dbFieldTables[field.model];

                if (dbFieldTable) {
                    var dbFieldValue = tableDbFieldValue(field, index);

                    var item = $scope.backupItem;

                    var model = item[field.model];

                    if (!$common.isValidJavaIdentifier(dbFieldTable.msg + ' java name', dbFieldValue.javaName, $table.tableFieldId(index, 'JavaName' + dbFieldTable.id)))
                        return false;

                    if ($common.isDefined(model)) {
                        var idx = _.findIndex(model, function (dbMeta) {
                            return dbMeta.databaseName == dbFieldValue.databaseName;
                        });

                        // Found duplicate.
                        if (idx >= 0 && index != idx)
                            return showPopoverMessage(null, null, $table.tableFieldId(index, 'DatabaseName' + dbFieldTable.id), 'Field with such database name already exists!');

                        idx = _.findIndex(model, function (dbMeta) {
                            return dbMeta.javaName == dbFieldValue.javaName;
                        });

                        // Found duplicate.
                        if (idx >= 0 && index != idx)
                            return showPopoverMessage(null, null, $table.tableFieldId(index, 'JavaName' + dbFieldTable.id), 'Field with such java name already exists!');

                        if (index < 0) {
                            model.push(dbFieldValue);
                        }
                        else {
                            var dbField = model[index];

                            dbField.databaseName = dbFieldValue.databaseName;
                            dbField.databaseType = dbFieldValue.databaseType;
                            dbField.javaName = dbFieldValue.javaName;
                            dbField.javaType = dbFieldValue.javaType;
                        }
                    }
                    else {
                        model = [dbFieldValue];

                        item[field.model] = model;
                    }

                    if (index < 0)
                        $table.tableNewItem(field);
                    else  if (index < model.length - 1)
                        $table.tableStartEdit(item, field, index + 1);
                    else
                        $table.tableNewItem(field);
                }
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

            $scope.tableIndexSave = function (field, curIdx) {
                var indexName = tableIndexName(field, curIdx);
                var indexType = tableIndexType(field, curIdx);

                var item = $scope.backupItem;

                var indexes = item.indexes;

                if ($common.isDefined(indexes)) {
                    var idx = _.findIndex(indexes, function (index) {
                        return index.name == indexName;
                    });

                    // Found duplicate.
                    if (idx >= 0 && idx != curIdx)
                        return showPopoverMessage(null, null, $table.tableFieldId(curIdx, 'IndexName'), 'Index with such name already exists!');
                }

                if (curIdx < 0) {
                    var newIndex = {name: indexName, type: indexType};

                    if (item.indexes)
                        item.indexes.push(newIndex);
                    else
                        item.indexes = [newIndex];
                }
                else {
                    item.indexes[curIdx].name = indexName;
                    item.indexes[curIdx].type = indexType;
                }

                if (curIdx < 0)
                    $scope.tableIndexNewItem(field, item.indexes.length - 1);
                else {
                    var index = item.indexes[curIdx];

                    if (index.fields && index.fields.length > 0)
                        $scope.tableIndexItemStartEdit(field, curIdx, 0);
                    else
                        $scope.tableIndexNewItem(field, curIdx);
                }
            };

            $scope.tableIndexNewItem = function (field, indexIdx) {
                var indexName = $scope.backupItem.indexes[indexIdx].name;

                $table.tableNewItem({ui: 'table-index-fields', model: indexName});

                field.newFieldName = null;
                field.newDirection = true;
            };

            $scope.tableIndexNewItemActive = function (itemIndex) {
                var indexes = $scope.backupItem.indexes;

                if (indexes) {
                    var index = indexes[itemIndex];

                    if (index)
                        return $table.tableNewItemActive({model: index.name});
                }

                return false;
            };

            $scope.tableIndexItemEditing = function (itemIndex, curIdx) {
                var indexes = $scope.backupItem.indexes;

                if (indexes) {
                    var index = indexes[itemIndex];

                    if (index)
                        return $table.tableEditing({model: index.name}, curIdx);
                }

                return false;
            };

            function tableIndexItemValue(field, index) {
                return index < 0
                    ? {name: field.newFieldName, direction: field.newDirection}
                    : {name: field.curFieldName, direction: field.curDirection};
            }

            $scope.tableIndexItemStartEdit = function (field, indexIdx, curIdx) {
                var index = $scope.backupItem.indexes[indexIdx];

                $table.tableState(index.name, curIdx);

                var indexItem = index.fields[curIdx];

                field.curFieldName = indexItem.name;
                field.curDirection = indexItem.direction;

                $focus('curFieldName');
            };

            $scope.tableIndexItemSaveVisible = function (field, index) {
                return !$common.isEmptyString(tableIndexItemValue(field, index).name);
            };

            $scope.tableIndexItemSave = function (field, indexIdx, curIdx) {
                var indexItemValue = tableIndexItemValue(field, curIdx);

                var fields = $scope.backupItem.indexes[indexIdx].fields;

                if ($common.isDefined(fields)) {
                    var idx = _.findIndex(fields, function (field) {
                        return field.name == indexItemValue.name;
                    });

                    // Found duplicate.
                    if (idx >= 0 && idx != curIdx)
                        return showPopoverMessage(null, null, $table.tableFieldId(curIdx, 'FieldName'), 'Field with such name already exists in index!');
                }

                var index = $scope.backupItem.indexes[indexIdx];

                if (curIdx < 0) {
                    if (index.fields)
                        index.fields.push(indexItemValue);
                    else
                        index.fields = [indexItemValue];

                    $scope.tableIndexNewItem(field, indexIdx);
                }
                else {
                    index.fields[curIdx] = indexItemValue;

                    if (curIdx < index.fields.length - 1)
                        $scope.tableIndexItemStartEdit(field, indexIdx, curIdx + 1);
                    else
                        $scope.tableIndexNewItem(field, indexIdx);
                }
            };

            $scope.tableRemoveIndexItem = function (index, curIdx) {
                $table.tableReset();

                index.fields.splice(curIdx, 1);
            };

            $scope.resetItem = function (group) {
                var resetTo = $scope.selectedItem;

                if (!$common.isDefined(resetTo))
                    resetTo = prepareNewItem();

                $common.resetItem($scope.backupItem, resetTo, $scope.metadata, group);
            };

            $scope.resetAll = function() {
                $table.tableReset();

                $confirm.confirm('Are you sure you want to reset current metadata?')
                    .then(function() {
                        $scope.backupItem = $scope.selectedItem ? angular.copy($scope.selectedItem) : prepareNewItem();
                    });
            };
        }]
);
