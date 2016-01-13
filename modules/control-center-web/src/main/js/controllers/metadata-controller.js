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
consoleModule.controller('metadataController', function ($filter, $http, $timeout, $state, $scope, $controller, $modal,
     $common,  $focus, $confirm, $confirmBatch, $clone, $table, $preview, $loading, $unsavedChangesGuard, $agentDownload) {
        $unsavedChangesGuard.install($scope);

        // Initialize the super class and extend it.
        angular.extend(this, $controller('save-remove', {$scope: $scope}));

        $scope.ui = $common.formUI();

        $scope.ui.packageName = $commonUtils.toJavaPackageName($scope.$root.user.email.replace('@', '.')
            .split('.').reverse().join('.') + '.model');
        $scope.ui.builtinKeys = true;
        $scope.ui.usePrimitives = true;
        $scope.ui.generateCaches = true;
        $scope.ui.generatedCachesClusters = [];

        $scope.removeDemoDropdown = [{ 'text': 'Remove Demo data', 'click': 'removeDemoItems()'}];

        $scope.removeDemoItems = function () {
            $table.tableReset();

            $confirm.confirm('Are you sure you want to remove all demo metadata and caches?')
                .then(function () {
                    $http.post('/api/v1/configuration/metadata/remove/demo')
                        .success(function () {
                            $common.showInfo('All demo metadata and caches have been removed');

                            $http.post('/api/v1/configuration/metadata/list')
                                .success(function (data) {
                                    $scope.spaces = data.spaces;
                                    $scope.clusters = data.clusters;
                                    $scope.caches = data.caches;
                                    $scope.metadatas = data.metadatas;

                                    $scope.ui.generatedCachesClusters = [];

                                    _.forEach($scope.clusters, function (cluster) {
                                        $scope.ui.generatedCachesClusters.push(cluster.value);
                                    });
                                });

                            $scope.selectItem(undefined, undefined);
                        })
                        .error(function (errMsg) {
                            $common.showError(errMsg);
                        });
                });
        };

        $scope.joinTip = $common.joinTip;
        $scope.getModel = $common.getModel;
        $scope.javaBuildInClasses = $common.javaBuildInClasses;
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

        var INFO_CONNECT_TO_DB = 'Configure connection to database';
        var INFO_SELECT_SCHEMAS = 'Select schemas to load tables from';
        var INFO_SELECT_TABLES = 'Select tables to load as cache type metadata';
        var INFO_SELECT_OPTIONS = 'Select load metadata options';
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

        var _dbPresets = [
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

        $scope.selectedPreset = {
            db: 'unknown',
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

    function _loadPresets () {
            try {
               var restoredPresets =  JSON.parse(localStorage.dbPresets);

                _.forEach(restoredPresets, function (restoredPreset) {
                    var preset = _.find(_dbPresets, { jdbcDriverClass: restoredPreset.jdbcDriverClass });

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

        function _savePreset (preset) {
            try {
                var oldPreset = _.find(_dbPresets, { jdbcDriverClass: preset.jdbcDriverClass });

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
            if (val && !$scope.loadMeta.demo) {
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
                    db: 'unknown',
                    jdbcUrl: 'jdbc:[database]',
                    user: 'admin'
                };

            result.jdbcDriverJar = selectedJdbcJar.jdbcDriverJar;
            result.jdbcDriverClass = selectedJdbcJar.jdbcDriverClass;

            return result;
        }

        $scope.supportedJdbcTypes = $common.mkOptions($common.SUPPORTED_JDBC_TYPES);

        $scope.supportedJavaTypes = $common.mkOptions($common.javaBuildInTypes);

        $scope.sortDirections = [
            {value: true, label: 'ASC'},
            {value: false, label: 'DESC'}
        ];

        $scope.panels = { activePanels: [0, 1] };

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
                $scope.loadMeta.allSchemasSelected = $scope.loadMeta.displayedSchemas.length > 0 &&
                    _.every($scope.loadMeta.displayedSchemas, 'use', true);
        };

        $scope.selectAllTables = function () {
            var allSelected = $scope.loadMeta.allTablesSelected;

            _.forEach($scope.loadMeta.displayedTables, function (table) {
                table.use = allSelected;
            });
        };

        $scope.selectTable = function () {
            if ($common.isDefined($scope.loadMeta) && $common.isDefined($scope.loadMeta.displayedTables))
                $scope.loadMeta.allTablesSelected = $scope.loadMeta.displayedTables.length > 0 &&
                    _.every($scope.loadMeta.displayedTables, 'use', true);
        };

        $scope.$watch('loadMeta.displayedSchemas', $scope.selectSchema);

        $scope.$watch('loadMeta.displayedTables', $scope.selectTable);

        // Pre-fetch modal dialogs.
        var loadMetaModal = $modal({scope: $scope, templateUrl: '/configuration/metadata-load.html', show: false});

        var hideLoadMetadata = loadMetaModal.hide;

        loadMetaModal.hide = function () {
            $agentDownload.stopAwaitAgent();

            hideLoadMetadata();
        };

        /**
         * Show load metadata modal.
         *
         * @param demo If 'true' then load metadata from demo database.
         */
        $scope.showLoadMetadataModal = function (demo) {
            $table.tableReset();

            $common.confirmUnsavedChanges($scope.ui.isDirty(), function () {
                if ($scope.ui.isDirty())
                    $scope.backupItem = $scope.selectedItem ? angular.copy($scope.selectedItem) : prepareNewItem();

                $scope.loadMeta = {
                    demo: demo,
                    action: 'drivers',
                    schemas: [],
                    allSchemasSelected: false,
                    tables: [],
                    allTablesSelected: false,
                    button: 'Next',
                    info: ''
                };

                $scope.loadMeta.loadingOptions = LOADING_JDBC_DRIVERS;

                $agentDownload.awaitAgent(function (result, onSuccess, onException) {
                    loadMetaModal.$promise.then(loadMetaModal.show);

                    // Get available JDBC drivers via agent.
                    if ($scope.loadMeta.action === 'drivers') {
                        $loading.start('loadingMetadataFromDb');

                        $scope.jdbcDriverJars = [];
                        $scope.ui.selectedJdbcDriverJar = {};

                        $http.post('/api/v1/agent/drivers')
                            .success(function (drivers) {
                                onSuccess();

                                if ($scope.loadMeta.demo) {
                                    $scope.ui.packageNamePrev = $scope.ui.packageName;
                                    $scope.ui.packageName = 'org.apache.ignite.console.demo.model';
                                }
                                else if ($scope.ui.packageNamePrev) {
                                    $scope.ui.packageName = $scope.ui.packageNamePrev;
                                    $scope.ui.packageNamePrev = null;
                                }

                                if (drivers && drivers.length > 0) {
                                    drivers = _.sortBy(drivers, 'jdbcDriverJar');

                                    if ($scope.loadMeta.demo) {
                                        var _h2DrvJar = _.find(drivers, function (drv) {
                                            return drv.jdbcDriverJar.startsWith('h2');
                                        });

                                        if (_h2DrvJar) {
                                            $scope.demoConnection.db = 'H2';
                                            $scope.demoConnection.jdbcDriverJar = _h2DrvJar.jdbcDriverJar;
                                        }
                                        else
                                            $scope.demoConnection.db = 'unknown';
                                    }
                                    else {
                                        drivers.forEach(function (driver) {
                                            $scope.jdbcDriverJars.push({
                                                label: driver.jdbcDriverJar,
                                                value: {
                                                    jdbcDriverJar: driver.jdbcDriverJar,
                                                    jdbcDriverClass: driver.jdbcDriverClass
                                                }
                                            });
                                        });

                                        $scope.ui.selectedJdbcDriverJar = $scope.jdbcDriverJars[0].value;
                                    }

                                    $common.confirmUnsavedChanges($scope.ui.isDirty(), function () {
                                        loadMetaModal.$promise.then(function () {
                                            $scope.loadMeta.action = 'connect';
                                            $scope.loadMeta.tables = [];
                                            $scope.loadMeta.loadingOptions = LOADING_SCHEMAS;

                                            $focus('jdbcUrl');
                                        });
                                    });
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
            });
        };

        /**
         * Load list of database schemas.
         */
        function _loadSchemas() {
            $loading.start('loadingMetadataFromDb');

            var preset = $scope.loadMeta.demo ? $scope.demoConnection : $scope.selectedPreset;

            if (!$scope.loadMeta.demo)
                _savePreset(preset);

            $http.post('/api/v1/agent/schemas', preset)
                .success(function (schemas) {
                    $scope.loadMeta.schemas = _.map(schemas, function (schema) {
                        return {use: false, name: schema};
                    });

                    $scope.loadMeta.action = 'schemas';

                    if ($scope.loadMeta.schemas.length === 0)
                        $scope.loadMetadataNext();
                    else
                        _.forEach($scope.loadMeta.schemas, function (sch) {
                            sch.use = true;
                        });

                    $scope.loadMeta.info = INFO_SELECT_SCHEMAS;
                    $scope.loadMeta.loadingOptions = LOADING_TABLES;
                })
                .error(function (errMsg) {
                    $common.showError(errMsg);
                })
                .finally(function() {
                    $loading.finish('loadingMetadataFromDb');
                });
        }

        /**
         * Load list of database tables.
         */
        function _loadTables() {
            $loading.start('loadingMetadataFromDb');

            $scope.loadMeta.allTablesSelected = false;

            var preset = $scope.loadMeta.demo ? $scope.demoConnection : $scope.selectedPreset;

            preset.schemas = [];

            _.forEach($scope.loadMeta.schemas, function (schema) {
                if (schema.use)
                    preset.schemas.push(schema.name);
            });

            $http.post('/api/v1/agent/metadata', preset)
                .success(function (tables) {
                    tables.forEach(function (tbl) {
                        Object.defineProperty(tbl, 'label', {
                            get: function () {
                                return tbl.schema + '.' + tbl.tbl;
                            }
                        });

                        tbl.use = $common.isDefined(_.find(tbl.cols, function (col) {
                            return col.key;
                        }));
                    });

                    $scope.loadMeta.action = 'tables';
                    $scope.loadMeta.tables = tables;
                    $scope.loadMeta.info = INFO_SELECT_TABLES;
                    $scope.loadMeta.loadingOptions = LOADING_METADATA;
                })
                .error(function (errMsg) {
                    $common.showError(errMsg);
                })
                .finally(function() {
                    $loading.finish('loadingMetadataFromDb');
                });
        }

        /**
         * Show page with load metadata options.
         */
        function _selectOptions() {
            $scope.loadMeta.action = 'options';
            $scope.loadMeta.button = 'Save';
            $scope.loadMeta.info = INFO_SELECT_OPTIONS;
        }

        function toJavaClassName(name) {
            var len = name.length;

            var buf = '';

            var capitalizeNext = true;

            for (var i = 0; i < len; i++) {
                var ch = name.charAt(i);

                if (ch === ' ' ||  ch === '_')
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
                $loading.start('loadingMetadataFromDb');

                $http.post('/api/v1/configuration/metadata/save/batch', batch)
                    .success(function (savedBatch) {
                        var lastItem;
                        var newItems = [];

                        _.forEach(savedBatch.generatedCaches, function (generatedCache) {
                            $scope.caches.push(generatedCache);
                        });

                        _.forEach(savedBatch.savedMetas, function (savedItem) {
                            var idx = _.findIndex($scope.metadatas, function (meta) {
                                return meta._id === savedItem._id;
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

            var batch = [];
            var tables = [];
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

                            indexes.push(
                                {name: idx.name, indexType: 'SORTED', fields: _.map(fields, function (fieldName) {
                                    return {
                                        name: toJavaName(fieldName),
                                        direction: !idx.fields[fieldName]
                                    };
                                })});
                        });
                    }

                    var metaFound = _.find($scope.metadatas, function (meta) {
                        return meta.valueType === valType;
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

                    meta.keyType = valType + 'Key' + dupSfx;
                    meta.valueType = valType + dupSfx;
                    meta.databaseSchema = table.schema;
                    meta.databaseTable = tableName;
                    meta.fields = qryFields;
                    meta.indexes = indexes;
                    meta.keyFields = keyFields;
                    meta.valueFields = valFields;
                    meta.demo = $scope.loadMeta.demo;

                    // Use Java built-in type for key.
                    if ($scope.ui.builtinKeys && meta.keyFields.length === 1)
                        meta.keyType = meta.keyFields[0].jdbcType.javaType;

                    // Prepare caches for generation.
                    if ($scope.ui.generateCaches)
                        meta.newCache = {
                            name: typeName,
                            clusters: $scope.ui.generatedCachesClusters,
                            demo: $scope.loadMeta.demo
                        };

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
                                return !item.skip;
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
                    .then(function () { checkOverwrite(); });
        }

        $scope.loadMetadataNext = function () {
            if (!$scope.nextAvailable())
                return;

            if ($scope.loadMeta.action === 'connect')
                _loadSchemas();
            else if ($scope.loadMeta.action === 'schemas')
                _loadTables();
            else if ($scope.loadMeta.action === 'tables')
                _selectOptions();
            else if ($scope.loadMeta.action === 'options')
                _saveMetadata();
        };

        $scope.nextTooltipText = function () {
            if ($scope.nextAvailable())
                return 'Click to continue';

            if ($scope.loadMeta.action === 'connect')
                return 'Resolve issue with H2 database driver<br>Close this dialog and try again';

            if ($scope.loadMeta.action === 'schemas')
                return 'Select schemas to continue';

            if ($scope.loadMeta.action === 'tables')
                return 'Select tables to continue';
        };

        $scope.nextAvailable = function () {
            var res = true;

            switch ($scope.loadMeta.action) {
                case 'connect':
                    if ($scope.loadMeta.demo)
                        res = $common.isDefined($scope.demoConnection.db === 'H2');

                    break;

                case 'schemas':
                    res = $common.isEmptyArray($scope.loadMeta.schemas) || $('#metadataSchemaData').find(':checked').length > 0;

                    break;

                case 'tables':
                    res = $('#metadataTableData').find(':checked').length > 0;

                    break;
            }

            return res;
        };

        $scope.loadMetadataPrev = function () {
            $scope.loadMeta.button = 'Next';

            if  ($scope.loadMeta.action === 'options') {
                $scope.loadMeta.action = 'tables';
                $scope.loadMeta.info = INFO_SELECT_TABLES;
            }
            else if  ($scope.loadMeta.action === 'tables' && $scope.loadMeta.schemas.length > 0) {
                $scope.loadMeta.action = 'schemas';
                $scope.loadMeta.info = INFO_SELECT_SCHEMAS;
                $scope.loadMeta.loadingOptions = LOADING_TABLES;
            }
            else {
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

        function restoreSelection() {
            var lastSelectedMetadata = angular.fromJson(sessionStorage.lastSelectedMetadata);

            if (lastSelectedMetadata) {
                var idx = _.findIndex($scope.metadatas, function (metadata) {
                    return metadata._id === lastSelectedMetadata;
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

        $http.post('/api/v1/configuration/metadata/list')
            .success(function (data) {
                $scope.spaces = data.spaces;
                $scope.clusters = data.clusters;
                $scope.caches = data.caches;
                $scope.metadatas = data.metadatas;

                _.forEach($scope.clusters, function (cluster) {
                    $scope.ui.generatedCachesClusters.push(cluster.value);
                });

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
                        else
                            restoreSelection();

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

                if ($common.getQueryVariable('new'))
                    $state.go('base.configuration.metadata');
            }

            $common.confirmUnsavedChanges($scope.ui.isDirty(), selectItem);

            $scope.ui.formTitle = $common.isDefined($scope.backupItem) && $scope.backupItem._id ?
                'Selected metadata: ' + $scope.backupItem.valueType
                : 'New metadata';
        };

        function prepareNewItem(cacheId) {
            return {
                space: $scope.spaces[0]._id,
                caches: cacheId && _.find($scope.caches, {value: cacheId}) ? [cacheId] :
                    (!$common.isEmptyArray($scope.caches) ? [$scope.caches[0].value] : [])
            };
        }

        // Add new metadata.
        $scope.createItem = function (cacheId) {
            if ($scope.tableReset(true)) {
                $timeout(function () {
                    $common.ensureActivePanel($scope.panels, 'query');
                    $common.ensureActivePanel($scope.panels, 'general', 'keyType');
                });

                $scope.selectItem(undefined, prepareNewItem(cacheId));
            }
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
                if ($common.isEmptyArray(item.fields))
                    return showPopoverMessage($scope.panels, 'query', 'fields-legend', 'Query fields should not be empty');

                var indexes = item.indexes;

                if (indexes && indexes.length > 0) {
                    if (_.find(indexes, function(index, i) {
                        if ($common.isEmptyArray(index.fields))
                            return !showPopoverMessage($scope.panels, 'query', 'indexes' + i, 'Index fields are not specified');
                    }))
                        return false;
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

            $http.post('/api/v1/configuration/metadata/save', item)
                .success(function (res) {
                    $scope.ui.markPristine();

                    var savedMeta = res[0];

                    var idx = _.findIndex($scope.metadatas, function (metadata) {
                        return metadata._id === savedMeta._id;
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
            if ($scope.tableReset(true)) {
                var item = $scope.backupItem;

                if (validate(item))
                    save(item);
            }
        };

        // Save cache type metadata with new name.
        $scope.cloneItem = function () {
            if ($scope.tableReset(true)) {
                if (validate($scope.backupItem))
                    $clone.confirm($scope.backupItem.valueType).then(function (newName) {
                        var item = angular.copy($scope.backupItem);

                        item._id = undefined;
                        item.valueType = newName;

                        save(item);
                    });
            }
        };

        // Remove metadata from db.
        $scope.removeItem = function () {
            $table.tableReset();

            var selectedItem = $scope.selectedItem;

            $confirm.confirm('Are you sure you want to remove cache type metadata: "' + selectedItem.valueType + '"?')
                .then(function () {
                        var _id = selectedItem._id;

                        $http.post('/api/v1/configuration/metadata/remove', {_id: _id})
                            .success(function () {
                                $common.showInfo('Cache type metadata has been removed: ' + selectedItem.valueType);

                                var metadatas = $scope.metadatas;

                                var idx = _.findIndex(metadatas, function (metadata) {
                                    return metadata._id === _id;
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

                                    $scope.ui.showValid = validFilter($scope.metadatas, false, true).length === 0;
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
                        $http.post('/api/v1/configuration/metadata/remove/all')
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

        $scope.toggleValid = function () {
            $scope.ui.showValid = !$scope.ui.showValid;

            var validFilter = $filter('metadatasValidation');

            var idx = _.findIndex(validFilter($scope.metadatas, $scope.ui.showValid, true), function (metadata) {
                return metadata._id === $scope.selectedItem._id;
            });

            if (idx === -1)
                $scope.selectItem(undefined, undefined);
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
                        return pair[pairField.searchCol] === pairValue[pairField.valueCol];
                    });

                    // Found duplicate by key.
                    if (idx >= 0 && idx !== index)
                        return showPopoverMessage($scope.panels, 'query', $table.tableFieldId(index, pairField.idPrefix + pairField.id), 'Field with such ' + pairField.dupObjName + ' already exists!');
                }

                if (pairField.classValidation && !$common.isValidJavaClass(pairField.msg, pairValue.value, true, $table.tableFieldId(index, 'Value' + pairField.id)))
                    return $table.tableFocusInvalidField(index, 'Value' + pairField.id);
            }

            return true;
        };

        function tableDbFieldValue(field, index) {
            return (index < 0) ?
                {
                    databaseFieldName: field.newDatabaseFieldName,
                    databaseFieldType: field.newDatabaseFieldType,
                    javaFieldName: field.newJavaFieldName,
                    javaFieldType: field.newJavaFieldType
                }
                : {
                    databaseFieldName: field.curDatabaseFieldName,
                    databaseFieldType: field.curDatabaseFieldType,
                    javaFieldName: field.curJavaFieldName,
                    javaFieldType: field.curJavaFieldType
                };
        }

        $scope.tableDbFieldSaveVisible = function (field, index) {
            var dbFieldValue = tableDbFieldValue(field, index);

            return !$common.isEmptyString(dbFieldValue.databaseFieldName) && $common.isDefined(dbFieldValue.databaseFieldType) &&
                !$common.isEmptyString(dbFieldValue.javaFieldName) && $common.isDefined(dbFieldValue.javaFieldType);
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
                        return showPopoverMessage($scope.panels, 'store', $table.tableFieldId(index, 'DatabaseFieldName' + dbFieldTable.id), 'Field with such database name already exists!');

                    idx = _.findIndex(model, function (dbMeta) {
                        return dbMeta.javaFieldName === dbFieldValue.javaFieldName;
                    });

                    // Found duplicate.
                    if (idx >= 0 && index !== idx)
                        return showPopoverMessage($scope.panels, 'store', $table.tableFieldId(index, 'JavaFieldName' + dbFieldTable.id), 'Field with such java name already exists!');

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
                    return showPopoverMessage($scope.panels, 'query', $table.tableFieldId(curIdx, 'IndexName'), 'Index with such name already exists!');
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
                $table.tableFocusInvalidField('FieldName' + (index.indexType === 'SORTED' ? 'S' : ''), indexIdx);

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
            return index < 0 ?
                {name: field.newFieldName, direction: field.newDirection}
                : {name: field.curFieldName, direction: field.curDirection};
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
                    return showPopoverMessage($scope.panels, 'query', $table.tableFieldId(curIdx, 'FieldName' + (index.indexType === 'SORTED' ? 'S' : '') + indexIdx + (curIdx >= 0 ? '-' : '')), 'Field with such name already exists in index!');
            }

            $table.tableReset();

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

            field.indexIdx = -1;

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

            $common.resetItem($scope.backupItem, resetTo, $scope.metadata, group);
        };

        $scope.resetAll = function() {
            $table.tableReset();

            $confirm.confirm('Are you sure you want to undo all changes for current metadata?')
                .then(function() {
                    $scope.backupItem = $scope.selectedItem ? angular.copy($scope.selectedItem) : prepareNewItem();
                });
        };
    }
);
