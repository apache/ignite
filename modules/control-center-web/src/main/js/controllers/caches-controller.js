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

// Controller for Caches screen.
consoleModule.controller('cachesController', [
    '$scope', '$controller', '$filter', '$http', '$timeout', '$common', '$focus', '$confirm', '$message', '$clone', '$table', '$preview', '$loading', '$unsavedChangesGuard',
    function ($scope, $controller, $filter, $http, $timeout, $common, $focus, $confirm, $message, $clone, $table, $preview, $loading, $unsavedChangesGuard) {
            $unsavedChangesGuard.install($scope);

            // Initialize the super class and extend it.
            angular.extend(this, $controller('save-remove', {$scope: $scope}));

            $scope.ui = $common.formUI();

            $scope.showMoreInfo = $message.message;

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

            $scope.tablePairSave = $table.tablePairSave;
            $scope.tablePairSaveVisible = $table.tablePairSaveVisible;

            var previews = [];

            $scope.previewInit = function (preview) {
                previews.push(preview);

                $preview.previewInit(preview);
            };

            $scope.previewChanged = $preview.previewChanged;

            $scope.hidePopover = $common.hidePopover;

            var showPopoverMessage = $common.showPopoverMessage;

            $scope.atomicities = $common.mkOptions(['ATOMIC', 'TRANSACTIONAL']);

            $scope.cacheModes = $common.mkOptions(['PARTITIONED', 'REPLICATED', 'LOCAL']);

            $scope.atomicWriteOrderModes = $common.mkOptions(['CLOCK', 'PRIMARY']);

            $scope.writeSynchronizationMode = $common.mkOptions(['FULL_SYNC', 'FULL_ASYNC', 'PRIMARY_SYNC']);

            $scope.memoryModes = $common.mkOptions(['ONHEAP_TIERED', 'OFFHEAP_TIERED', 'OFFHEAP_VALUES']);

            $scope.evictionPolicies = [
                {value: 'LRU', label: 'LRU'},
                {value: 'RND', label: 'Random'},
                {value: 'FIFO', label: 'FIFO'},
                {value: 'SORTED', label: 'Sorted'},
                {value: undefined, label: 'Not set'}
            ];

            $scope.rebalanceModes = $common.mkOptions(['SYNC', 'ASYNC', 'NONE']);

            $scope.cacheStoreFactories = [
                {value: 'CacheJdbcPojoStoreFactory', label: 'JDBC POJO store factory'},
                {value: 'CacheJdbcBlobStoreFactory', label: 'JDBC BLOB store factory'},
                {value: 'CacheHibernateBlobStoreFactory', label: 'Hibernate BLOB store factory'},
                {value: undefined, label: 'Not set'}
            ];

            $scope.cacheStoreJdbcDialects = [
                {value: 'Generic', label: 'Generic JDBC'},
                {value: 'Oracle', label: 'Oracle'},
                {value: 'DB2', label: 'IBM DB2'},
                {value: 'SQLServer', label: 'Microsoft SQL Server'},
                {value: 'MySQL', label: 'MySQL'},
                {value: 'PostgreSQL', label: 'PostgreSQL'},
                {value: 'H2', label: 'H2 database'}
            ];

            $scope.toggleExpanded = function () {
                $scope.ui.expanded = !$scope.ui.expanded;

                $common.hidePopover();
            };

            $scope.panels = {activePanels: [0]};

            $scope.general = [];
            $scope.advanced = [];
            $scope.caches = [];
            $scope.metadatas = [];

            $scope.preview = {
                general: {xml: '', java: '', allDefaults: true},
                memory: {xml: '', java: '', allDefaults: true},
                query: {xml: '', java: '', allDefaults: true},
                store: {xml: '', java: '', allDefaults: true},
                concurrency: {xml: '', java: '', allDefaults: true},
                rebalance: {xml: '', java: '', allDefaults: true},
                serverNearCache: {xml: '', java: '', allDefaults: true},
                statistics: {xml: '', java: '', allDefaults: true}
            };

            $scope.required = function (field) {
                var model = $common.isDefined(field.path) ? field.path + '.' + field.model : field.model;

                var backupItem = $scope.backupItem;

                var memoryMode = backupItem.memoryMode;

                var onHeapTired = memoryMode == 'ONHEAP_TIERED';
                var offHeapTired = memoryMode == 'OFFHEAP_TIERED';

                var offHeapMaxMemory = backupItem.offHeapMaxMemory;

                if (model == 'offHeapMaxMemory' && offHeapTired)
                    return true;

                if (model == 'evictionPolicy.kind' && onHeapTired)
                    return backupItem.swapEnabled || ($common.isDefined(offHeapMaxMemory) && offHeapMaxMemory >= 0);

                return false;
            };

            $scope.tableSimpleValid = function (item, field, fx, index) {
                var model;

                switch (field.model) {
                    case 'hibernateProperties':
                        if (fx.indexOf('=') < 0)
                            return showPopoverMessage(null, null, $table.tableFieldId(index, 'HibProp'), 'Property should be present in format key=value!');

                        model = item.cacheStoreFactory.CacheHibernateBlobStoreFactory[field.model];

                        var key = fx.split('=')[0];

                        var exist = false;

                        if ($common.isDefined(model)) {
                            model.forEach(function (val) {
                                if (val.split('=')[0] == key)
                                    exist = true;
                            })
                        }

                        if (exist)
                            return showPopoverMessage(null, null, $table.tableFieldId(index, 'HibProp'), 'Property with such name already exists!');

                        break;

                    case 'sqlFunctionClasses':
                        if (!$common.isValidJavaClass('SQL function', fx, false, $table.tableFieldId(index, 'SqlFx')))
                            return $table.tableFocusInvalidField(index, 'SqlFx');

                        model = item[field.model];

                        if ($common.isDefined(model)) {
                            var idx = _.indexOf(model, fx);

                            // Found duplicate.
                            if (idx >= 0 && idx != index)
                                return showPopoverMessage(null, null, $table.tableFieldId(index, 'SqlFx'), 'SQL function with such class name already exists!');
                        }
                }

                return true;
            };

            $scope.tablePairValid = function (item, field, index) {
                var pairValue = $table.tablePairValue(field, index);

                if (!$common.isValidJavaClass('Indexed type key', pairValue.key, true, $table.tableFieldId(index, 'KeyIndexedType')))
                    return $table.tableFocusInvalidField(index, 'KeyIndexedType');

                if (!$common.isValidJavaClass('Indexed type value', pairValue.value, true, $table.tableFieldId(index, 'ValueIndexedType')))
                    return $table.tableFocusInvalidField(index, 'ValueIndexedType');

                var model = item[field.model];

                if ($common.isDefined(model)) {
                    var idx = _.findIndex(model, function (pair) {
                        return pair.keyClass == pairValue.key && pair.valueClass == pairValue.value;
                    });

                    // Found duplicate.
                    if (idx >= 0 && idx != index)
                        return showPopoverMessage(null, null, $table.tableFieldId(index, 'ValueIndexedType'), 'Indexed type with such key and value classes already exists!');
                }

                return true;
            };

            function selectFirstItem() {
                if ($scope.caches.length > 0)
                    $scope.selectItem($scope.caches[0]);
            }

            function cacheMetadatas(item) {
                return _.reduce($scope.metadatas, function (memo, meta) {
                    if (item && _.contains(item.metadatas, meta.value)) {
                        memo.push(meta.meta);
                    }

                    return memo;
                }, []);
            }

            $loading.start('loadingCachesScreen');

            // When landing on the page, get caches and show them.
            $http.post('caches/list')
                .success(function (data) {
                    var validFilter = $filter('metadatasValidation');

                    $scope.spaces = data.spaces;
                    $scope.caches = data.caches;
                    $scope.clusters = data.clusters;
                    $scope.metadatas = _.sortBy(_.map(validFilter(data.metadatas, true, false), function (meta) {
                        return {value: meta._id, label: meta.valueType, kind: meta.kind, meta: meta}
                    }), 'label');

                    // Load page descriptor.
                    $http.get('/models/caches.json')
                        .success(function (data) {
                            $scope.screenTip = data.screenTip;
                            $scope.moreInfo = data.moreInfo;
                            $scope.general = data.general;
                            $scope.advanced = data.advanced;

                            $scope.ui.addGroups(data.general, data.advanced);

                            if ($common.getQueryVariable('new'))
                                $scope.createItem($common.getQueryVariable('id'));
                            else {
                                var lastSelectedCache = angular.fromJson(sessionStorage.lastSelectedCache);

                                if (lastSelectedCache) {
                                    var idx = _.findIndex($scope.caches, function (cache) {
                                        return cache._id == lastSelectedCache;
                                    });

                                    if (idx >= 0)
                                        $scope.selectItem($scope.caches[idx]);
                                    else {
                                        sessionStorage.removeItem('lastSelectedCache');

                                        selectFirstItem();
                                    }
                                }
                                else
                                    selectFirstItem();
                            }

                            $scope.$watch('backupItem', function (val) {
                                if (val) {
                                    var srcItem = $scope.selectedItem ? $scope.selectedItem : prepareNewItem();

                                    $scope.ui.checkDirty(val, srcItem);

                                    var metas = cacheMetadatas(val);
                                    var varName = $commonUtils.toJavaName('cache', val.name);

                                    $scope.preview.general.xml = $generatorXml.cacheMetadatas(metas, $generatorXml.cacheGeneral(val)).asString();
                                    $scope.preview.general.java = $generatorJava.cacheMetadatas(metas, varName, $generatorJava.cacheGeneral(val, varName)).asString();
                                    $scope.preview.general.allDefaults = $common.isEmptyString($scope.preview.general.xml);

                                    $scope.preview.memory.xml = $generatorXml.cacheMemory(val).asString();
                                    $scope.preview.memory.java = $generatorJava.cacheMemory(val, varName).asString();
                                    $scope.preview.memory.allDefaults = $common.isEmptyString($scope.preview.memory.xml);

                                    $scope.preview.query.xml = $generatorXml.cacheQuery(val).asString();
                                    $scope.preview.query.java = $generatorJava.cacheQuery(val, varName).asString();
                                    $scope.preview.query.allDefaults = $common.isEmptyString($scope.preview.query.xml);

                                    var storeFactory = $generatorXml.cacheStore(val, metas);

                                    $scope.preview.store.xml = $generatorXml.generateDataSources(storeFactory.datasources).asString() + storeFactory.asString();
                                    $scope.preview.store.java = $generatorJava.cacheStore(val, metas, varName).asString();
                                    $scope.preview.store.allDefaults = $common.isEmptyString($scope.preview.store.xml);

                                    $scope.preview.concurrency.xml = $generatorXml.cacheConcurrency(val).asString();
                                    $scope.preview.concurrency.java = $generatorJava.cacheConcurrency(val, varName).asString();
                                    $scope.preview.concurrency.allDefaults = $common.isEmptyString($scope.preview.concurrency.xml);

                                    $scope.preview.rebalance.xml = $generatorXml.cacheRebalance(val).asString();
                                    $scope.preview.rebalance.java = $generatorJava.cacheRebalance(val, varName).asString();
                                    $scope.preview.rebalance.allDefaults = $common.isEmptyString($scope.preview.rebalance.xml);

                                    $scope.preview.serverNearCache.xml = $generatorXml.cacheServerNearCache(val).asString();
                                    $scope.preview.serverNearCache.java = $generatorJava.cacheServerNearCache(val, varName).asString();
                                    $scope.preview.serverNearCache.allDefaults = $common.isEmptyString($scope.preview.serverNearCache.xml);

                                    $scope.preview.statistics.xml = $generatorXml.cacheStatistics(val).asString();
                                    $scope.preview.statistics.java = $generatorJava.cacheStatistics(val, varName).asString();
                                    $scope.preview.statistics.allDefaults = $common.isEmptyString($scope.preview.statistics.xml);
                                }
                            }, true);

                            $scope.$watch('backupItem.metadatas', function (val) {
                                var item = $scope.backupItem;

                                var cacheStoreFactory = $common.isDefined(item) &&
                                    $common.isDefined(item.cacheStoreFactory) &&
                                    $common.isDefined(item.cacheStoreFactory.kind);

                                if (val && !cacheStoreFactory) {
                                    if (_.findIndex(cacheMetadatas(item), $common.metadataForStoreConfigured) >= 0) {
                                        item.cacheStoreFactory.kind = 'CacheJdbcPojoStoreFactory';

                                        if (!item.readThrough && !item.writeThrough) {
                                            item.readThrough = true;
                                            item.writeThrough = true;
                                        }

                                        $timeout(function () {
                                            $common.ensureActivePanel($scope.panels, 'store');
                                        });
                                    }
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
                    $loading.finish('loadingCachesScreen');
                });

            $scope.selectItem = function (item, backup) {
                function selectItem() {
                    $table.tableReset();

                    $scope.selectedItem = angular.copy(item);

                    try {
                        if (item)
                            sessionStorage.lastSelectedCache = angular.toJson(item._id);
                        else
                            sessionStorage.removeItem('lastSelectedCache');
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

                $scope.ui.formTitle = $common.isDefined($scope.backupItem) && $scope.backupItem._id ?
                    'Selected cache: ' + $scope.backupItem.name : 'New cache';
            };

            function prepareNewItem(id) {
                return {
                    space: $scope.spaces[0]._id,
                    cacheMode: 'PARTITIONED',
                    atomicityMode: 'ATOMIC',
                    readFromBackup: true,
                    copyOnRead: true,
                    clusters: id && _.find($scope.clusters, {value: id}) ? [id] : [],
                    metadatas: id && _.find($scope.metadatas, {value: id}) ? [id] : []
                }
            }

            // Add new cache.
            $scope.createItem = function (id) {
                $table.tableReset();

                $timeout(function () {
                    $common.ensureActivePanel($scope.panels, 'general', 'cacheName');
                });

                $scope.selectItem(undefined, prepareNewItem(id));
            };

            // Check cache logical consistency.
            function validate(item) {
                if ($common.isEmptyString(item.name))
                    return showPopoverMessage($scope.panels, 'general', 'cacheName', 'Name should not be empty');

                if (item.memoryMode == 'OFFHEAP_TIERED' && item.offHeapMaxMemory == null)
                    return showPopoverMessage($scope.panels, 'memory', 'offHeapMaxMemory',
                        'Off-heap max memory should be specified');

                if (item.memoryMode == 'ONHEAP_TIERED' && item.offHeapMaxMemory > 0 &&
                        !$common.isDefined(item.evictionPolicy.kind)) {
                    return showPopoverMessage($scope.panels, 'memory', 'evictionPolicy', 'Eviction policy should not be configured');
                }

                var cacheStoreFactorySelected = item.cacheStoreFactory && item.cacheStoreFactory.kind;

                if (cacheStoreFactorySelected) {
                    if (item.cacheStoreFactory.kind == 'CacheJdbcPojoStoreFactory') {
                        if ($common.isEmptyString(item.cacheStoreFactory.CacheJdbcPojoStoreFactory.dataSourceBean))
                            return showPopoverMessage($scope.panels, 'store', 'dataSourceBean',
                                'Data source bean should not be empty');

                        if (!item.cacheStoreFactory.CacheJdbcPojoStoreFactory.dialect)
                            return showPopoverMessage($scope.panels, 'store', 'dialect',
                                'Dialect should not be empty');
                    }

                    if (item.cacheStoreFactory.kind == 'CacheJdbcBlobStoreFactory') {
                        if ($common.isEmptyString(item.cacheStoreFactory.CacheJdbcBlobStoreFactory.user))
                            return showPopoverMessage($scope.panels, 'store', 'user',
                                'User should not be empty');

                        if ($common.isEmptyString(item.cacheStoreFactory.CacheJdbcBlobStoreFactory.dataSourceBean))
                            return showPopoverMessage($scope.panels, 'store', 'dataSourceBean',
                                'Data source bean should not be empty');
                    }
                }

                if ((item.readThrough || item.writeThrough) && !cacheStoreFactorySelected)
                    return showPopoverMessage($scope.panels, 'store', 'cacheStoreFactory',
                        (item.readThrough ? 'Read' : 'Write') + ' through are enabled but store is not configured!');

                if (item.writeBehindEnabled && !cacheStoreFactorySelected)
                    return showPopoverMessage($scope.panels, 'store', 'cacheStoreFactory',
                        'Write behind enabled but store is not configured!');

                if (cacheStoreFactorySelected) {
                    if (!item.readThrough && !item.writeThrough)
                        return showPopoverMessage($scope.panels, 'store', 'readThrough',
                            'Store is configured but read/write through are not enabled!');

                    if (item.cacheStoreFactory.kind == 'CacheJdbcPojoStoreFactory') {
                        if ($common.isDefined(item.metadatas)) {
                            var metadatas = cacheMetadatas($scope.backupItem);

                            if (_.findIndex(metadatas, $common.metadataForStoreConfigured) < 0)
                                return showPopoverMessage($scope.panels, 'general', 'metadata',
                                    'Cache with configured JDBC POJO store factory should contain at least one metadata with store configuration');
                        }
                    }
                }

                return true;
            }

            // Save cache into database.
            function save(item) {
                $http.post('caches/save', item)
                    .success(function (_id) {
                        $scope.ui.markPristine();

                        var idx = _.findIndex($scope.caches, function (cache) {
                            return cache._id == _id;
                        });

                        if (idx >= 0)
                            angular.extend($scope.caches[idx], item);
                        else {
                            item._id = _id;
                            $scope.caches.push(item);
                        }

                        $scope.selectItem(item);

                        $common.showInfo('Cache "' + item.name + '" saved.');
                    })
                    .error(function (errMsg) {
                        $common.showError(errMsg);
                    });
            }

            // Save cache.
            $scope.saveItem = function () {
                $table.tableReset();

                var item = $scope.backupItem;

                if (validate(item))
                    save(item);
            };

            // Save cache with new name.
            $scope.cloneItem = function () {
                $table.tableReset();

                if (validate($scope.backupItem))
                    $clone.confirm($scope.backupItem.name).then(function (newName) {
                        var item = angular.copy($scope.backupItem);

                        item._id = undefined;
                        item.name = newName;

                        save(item);
                    });
            };

            // Remove cache from db.
            $scope.removeItem = function () {
                $table.tableReset();

                var selectedItem = $scope.selectedItem;

                $confirm.confirm('Are you sure you want to remove cache: "' + selectedItem.name + '"?')
                    .then(function () {
                            var _id = selectedItem._id;

                            $http.post('caches/remove', {_id: _id})
                                .success(function () {
                                    $common.showInfo('Cache has been removed: ' + selectedItem.name);

                                    var caches = $scope.caches;

                                    var idx = _.findIndex(caches, function (cache) {
                                        return cache._id == _id;
                                    });

                                    if (idx >= 0) {
                                        caches.splice(idx, 1);

                                        if (caches.length > 0)
                                            $scope.selectItem(caches[0]);
                                        else
                                            $scope.selectItem(undefined, undefined);
                                    }
                                })
                                .error(function (errMsg) {
                                    $common.showError(errMsg);
                                });
                    });
            };

            // Remove all caches from db.
            $scope.removeAllItems = function () {
                $table.tableReset();

                $confirm.confirm('Are you sure you want to remove all caches?')
                    .then(function () {
                            $http.post('caches/remove/all')
                                .success(function () {
                                    $common.showInfo('All caches have been removed');

                                    $scope.caches = [];

                                    $scope.selectItem(undefined, undefined);
                                })
                                .error(function (errMsg) {
                                    $common.showError(errMsg);
                                });
                    });
            };

            $scope.resetItem = function (group) {
                var resetTo = $scope.selectedItem;

                if (!$common.isDefined(resetTo))
                    resetTo = prepareNewItem();

                $common.resetItem($scope.backupItem, resetTo, $scope.general, group);
                $common.resetItem($scope.backupItem, resetTo, $scope.advanced, group);
            };

            $scope.resetAll = function() {
                $table.tableReset();

                $confirm.confirm('Are you sure you want to undo all changes for current cache?')
                    .then(function() {
                        $scope.backupItem = $scope.selectedItem ? angular.copy($scope.selectedItem) : prepareNewItem();
                    });
            };
        }]
);
