/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
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
                var idx = _.findIndex(jdbcDrivers, function (jdbcDriver) {
                    return  jdbcDriver.jdbcDriverJar == jdbcDriverJar;
                });

                if (idx >= 0) {
                    var jdbcDriverClass = jdbcDrivers[idx].jdbcDriverClass;

                    idx = _.findIndex(presets, function (preset) {
                        return preset.jdbcDriverClass == jdbcDriverClass;
                    });

                    if (idx >= 0)
                        return presets[idx];
                }

                return {
                    db: 'unknown',
                    jdbcDriverClass: '',
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
                {value: false, label: 'ASC'},
                {value: true, label: 'DESC'}
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
                $scope.loadMeta.allSchemasSelected = _.every($scope.loadMeta.schemas, 'use', true);
            };

            $scope.selectAllTables = function () {
                var allSelected = $scope.loadMeta.allTablesSelected;

                _.forEach($scope.loadMeta.displayedTables, function (table) {
                    table.use = allSelected;
                });
            };

            $scope.selectTable = function () {
                $scope.loadMeta.allTablesSelected = _.every($scope.loadMeta.tables, 'use', true);
            };

            // Pre-fetch modal dialogs.
            var loadMetaModal = $modal({scope: $scope, templateUrl: 'metadata/metadata-load', show: false});

            // Show load metadata modal.
            $scope.showLoadMetadataModal = function () {
                $scope.loadMeta = {
                    action: 'connect',
                    schemas: [],
                    allSchemasSelected: false,
                    tables: [],
                    allTablesSelected: false,
                    button: 'Next',
                    info: 'Configure connection to database'
                };

                function getDrivers(onSuccess, onException) {
                    // Get available JDBC drivers via agent.
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

                                        loadMetaModal.show();

                                        $focus('jdbcUrl');
                                    });
                                }

                                $common.confirmUnsavedChanges($scope.ui.isDirty(), openLoadMetadataModal);
                            }
                            else
                                $common.showError('JDBC drivers not found!');
                        })
                        .error(function (errMsg, status) {
                            onException(errMsg, status);
                        });
                }

                $scope.awaitAgent(getDrivers);
            };

            function _loadSchemas() {
                $loading.start('loadingMetadataFromDb');

                $http.post('/agent/schemas', $scope.preset)
                    .success(function (schemas) {
                        $scope.loadMeta.schemas = _.map(schemas, function (schema) { return {use: false, name: schema}});
                        $scope.loadMeta.action = 'schemas';
                    })
                    .error(function (errMsg) {
                        $common.showError(errMsg);
                    })
                    .finally(function() {
                        $scope.loadMeta.info = INFO_SELECT_SCHEMAS;

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

            $scope.ui.packageName = $scope.user.email.replace('@', '.').split('.').reverse().join('.');

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
                        var ascFields = [];
                        var descFields = [];
                        var groups = [];
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

                        function colType(colName) {
                            var col = _.find(table.cols, function (col) {
                                return col.name == colName;
                            });

                            if (col)
                                return $common.findJdbcType(col.type).javaType;

                            return 'Unknown';
                        }

                        var _containKey = false;

                        _.forEach(table.cols, function (col) {
                            var colName = col.name;
                            var jdbcType = $common.findJdbcType(col.type);

                            if (!fieldIndexed(colName, table))
                                qryFields.push(queryField(colName, jdbcType));

                            if (_.includes(table.ascCols, colName))
                                ascFields.push(queryField(colName, jdbcType));

                            if (_.includes(table.descCols, colName))
                                descFields.push(queryField(colName, jdbcType));

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
                            var indexes = Object.keys(idxs);

                            _.forEach(indexes, function (indexName) {
                                var index = idxs[indexName];

                                var fields = Object.keys(index);

                                if (fields.length > 1)
                                    groups.push(
                                        {name: indexName, fields: _.map(fields, function (fieldName) {
                                            return {
                                                name: fieldName,
                                                className: colType(fieldName),
                                                direction: index[fieldName]
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
                        meta.queryFields = qryFields;
                        meta.ascendingFields = ascFields;
                        meta.descendingFields = descFields;
                        meta.groups = groups;
                        meta.keyFields = keyFields;
                        meta.valueFields = valFields;

                        batch.push(meta);
                        tables.push(tableName);
                    }
                });

                /**
                 * Check field with specified name is indexed.
                 *
                 * @param name Field name.
                 * @param table Table to check indexed fields.
                 * @returns {boolean} <code>True</code> when field is indexed of <code>false</code> otherwise.
                 */
                function fieldIndexed(name, table) {
                    // Check if in asc or desc fields.
                    if (_.includes(table.ascCols, name) || _.includes(table.descCols, name) || !table.idxs)
                        return true;

                    return _.findKey(table.idxs, function(fields) { return _.includes(Object.keys(fields), name); }) != undefined;
                }

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
                    $confirm.confirm('Imported tables contain tables without primary key.<br/>' +
                        'You should manually configure key type and key fields for these metadata types.')
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
                }
                else if  ($scope.loadMeta.action == 'schemas') {
                    $scope.loadMeta.action = 'connect';
                    $scope.loadMeta.info = INFO_CONNECT_TO_DB;
                }
            };

            $scope.metadataTitle = function () {
                var validFilter = $filter('metadatasValidation');

                if ($scope.ui.showValid || validFilter($scope.displayedRows, false, true).length == 0)
                    return 'Types metadata:';

                return 'Type metadata without key fields:';
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
                                $scope.createItem();
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
                                    $scope.preview.store.java = $generatorJava.metadataStore(val).asString();
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

            function prepareNewItem() {
                return {space: $scope.spaces[0]._id, caches: []};
            }

            // Add new metadata.
            $scope.createItem = function () {
                $table.tableReset();

                $timeout(function () {
                    $common.ensureActivePanel($scope.panels, 'query');
                    $common.ensureActivePanel($scope.panels, 'general', 'keyType');
                });

                $scope.selectItem(undefined, prepareNewItem());
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
                    var groups = item.groups;

                    if (groups && groups.length > 0) {
                        for (var i = 0; i < groups.length; i++) {
                            var group = groups[i];
                            var fields = group.fields;

                            if ($common.isEmptyArray(fields))
                                return showPopoverMessage($scope.panels, 'query', 'groups' + i, 'Group fields are not specified');

                            if (fields.length == 1) {
                                return showPopoverMessage($scope.panels, 'query', 'groups' + i, 'Group has only one field. Consider to use ascending or descending fields.');
                            }
                        }
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

                if (validate(item))
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
                queryFields: {msg: 'Query field class', id: 'QryField'},
                ascendingFields: {msg: 'Ascending field class', id: 'AscField'},
                descendingFields: {msg: 'Descending field class', id: 'DescField'}
            };

            $scope.tablePairValid = function (item, field, index) {
                var pairField = pairFields[field.model];

                var pairValue = $table.tablePairValue(field, index);

                if (pairField) {
                    if (!$common.isValidJavaClass(pairField.msg, pairValue.value, true, $table.tableFieldId(index, 'Value' + pairField.id)))
                        return $table.tableFocusInvalidField(index, 'Value' + pairField.id);

                    var model = item[field.model];

                    if ($common.isDefined(model)) {
                        var idx = _.findIndex(model, function (pair) {
                            return pair.name == pairValue.key
                        });

                        // Found duplicate.
                        if (idx >= 0 && idx != index)
                            return showPopoverMessage(null, null, $table.tableFieldId(index, 'Key' + pairField.id), 'Field with such name already exists!');
                    }
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

            function tableGroupValue(field, index) {
                return index < 0 ? field.newGroupName : field.curGroupName;
            }

            $scope.tableGroupSaveVisible = function (field, index) {
                return !$common.isEmptyString(tableGroupValue(field, index));
            };

            $scope.tableGroupSave = function (field, index) {
                var groupName = tableGroupValue(field, index);

                var groups = $scope.backupItem.groups;

                if ($common.isDefined(groups)) {
                    var idx = _.findIndex(groups, function (group) {
                        return group.name == groupName;
                    });

                    // Found duplicate.
                    if (idx >= 0 && idx != index)
                        return showPopoverMessage(null, null, $table.tableFieldId(index, 'GroupName'), 'Group with such name already exists!');
                }

                var item = $scope.backupItem;

                if (index < 0) {
                    var newGroup = {name: groupName};

                    if (item.groups)
                        item.groups.push(newGroup);
                    else
                        item.groups = [newGroup];
                }
                else
                    item.groups[index].name = groupName;

                if (index < 0)
                    $scope.tableGroupNewItem(field, item.groups.length - 1);
                else {
                    var group = item.groups[index];

                    if (group.fields || group.fields.length > 0)
                        $scope.tableGroupItemStartEdit(field, index, 0);
                    else
                        $scope.tableGroupNewItem(field, index);
                }
            };

            $scope.tableGroupNewItem = function (field, groupIndex) {
                var groupName = $scope.backupItem.groups[groupIndex].name;

                $table.tableNewItem({ui: 'table-query-group-fields', model: groupName});

                field.newFieldName = null;
                field.newClassName = null;
                field.newDirection = false;
            };

            $scope.tableGroupNewItemActive = function (groupIndex) {
                var groups = $scope.backupItem.groups;

                if (groups) {
                    var group = groups[groupIndex];

                    if (group) {
                        var groupName = group.name;

                        return $table.tableNewItemActive({model: groupName});
                    }
                }

                return false;
            };

            $scope.tableGroupItemEditing = function (groupIndex, index) {
                var groups = $scope.backupItem.groups;

                if (groups) {
                    var group = groups[groupIndex];

                    if (group)
                        return $table.tableEditing({model: group.name}, index);
                }

                return false;
            };

            function tableGroupItemValue(field, index) {
                return index < 0
                    ? {name: field.newFieldName, className: field.newClassName, direction: field.newDirection}
                    : {name: field.curFieldName, className: field.curClassName, direction: field.curDirection};
            }

            $scope.tableGroupItemStartEdit = function (field, groupIndex, index) {
                var groups = $scope.backupItem.groups;

                var group = groups[groupIndex];

                $table.tableState(group.name, index);

                var groupItem = group.fields[index];

                field.curFieldName = groupItem.name;
                field.curClassName = groupItem.className;
                field.curDirection = groupItem.direction;

                $focus('curFieldName');
            };

            $scope.tableGroupItemSaveVisible = function (field, index) {
                var groupItemValue = tableGroupItemValue(field, index);

                return !$common.isEmptyString(groupItemValue.name) && !$common.isEmptyString(groupItemValue.className);
            };

            $scope.tableGroupItemSave = function (field, groupIndex, index) {
                var groupItemValue = tableGroupItemValue(field, index);

                if (!$common.isValidJavaClass('Group field', groupItemValue.className, true, $table.tableFieldId(index, 'ClassName')))
                    return $table.tableFocusInvalidField(index, 'ClassName');

                var fields = $scope.backupItem.groups[groupIndex].fields;

                if ($common.isDefined(fields)) {
                    var idx = _.findIndex(fields, function (field) {
                        return field.name == groupItemValue.name;
                    });

                    // Found duplicate.
                    if (idx >= 0 && idx != index)
                        return showPopoverMessage(null, null, $table.tableFieldId(index, 'FieldName'), 'Field with such name already exists in group!');
                }

                var group = $scope.backupItem.groups[groupIndex];

                if (index < 0) {
                    if (group.fields)
                        group.fields.push(groupItemValue);
                    else
                        group.fields = [groupItemValue];

                    $scope.tableGroupNewItem(field, groupIndex);
                }
                else {
                    var groupItem = group.fields[index];

                    groupItem.name = groupItemValue.name;
                    groupItem.className = groupItemValue.className;
                    groupItem.direction = groupItemValue.direction;

                    if (index < group.fields.length - 1)
                        $scope.tableGroupItemStartEdit(field, groupIndex, index + 1);
                    else
                        $scope.tableGroupNewItem(field, groupIndex);
                }
            };

            $scope.tableRemoveGroupItem = function (group, index) {
                $table.tableReset();

                group.fields.splice(index, 1);
            };

            $scope.resetItem = function (group) {
                var resetTo = $scope.selectedItem;

                if (!$common.isDefined(resetTo))
                    resetTo = prepareNewItem();

                $common.resetItem($scope.backupItem, resetTo, $scope.metadata, group);
            }
        }]
);
