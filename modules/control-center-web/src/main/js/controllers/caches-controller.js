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
    '$scope', '$state', '$controller', '$filter', '$http', '$timeout', '$common', '$focus', '$confirm', '$clone', '$table', '$preview', '$loading', '$unsavedChangesGuard',
    function ($scope, $state, $controller, $filter, $http, $timeout, $common, $focus, $confirm, $clone, $table, $preview, $loading, $unsavedChangesGuard) {
        $unsavedChangesGuard.install($scope);

        // Initialize the super class and extend it.
        angular.extend(this, $controller('save-remove', {$scope: $scope}));

        $scope.ui = $common.formUI();
        $scope.ui.activePanels = [0];
        $scope.ui.topPanels = [0, 1, 2, 3];

        $scope.selectedItemWatchGuard = false;

        $scope.joinTip = $common.joinTip;
        $scope.getModel = $common.getModel;
        $scope.javaBuiltInClasses = $common.javaBuiltInClasses;
        $scope.compactJavaName = $common.compactJavaName;
        $scope.widthIsSufficient = $common.widthIsSufficient;
        $scope.saveBtnTipText = $common.saveBtnTipText;
        $scope.panelExpanded = $common.panelExpanded;

        $scope.tableVisibleRow = $table.tableVisibleRow;

        $scope.tableSave = function (field, index, stopEdit) {
            switch (field.type) {
                case 'table-simple':
                    if ($table.tableSimpleSaveVisible(field, index))
                        return $table.tableSimpleSave($scope.tableSimpleValid, $scope.backupItem, field, index, stopEdit);

                    break;
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

        $scope.tablePairSave = $table.tablePairSave;
        $scope.tablePairSaveVisible = $table.tablePairSaveVisible;

        $scope.tableEditedRowIndex = $table.tableEditedRowIndex;

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

        $scope.jdbcBlobStoreConnections = [
            {value: 'URL', label: 'URL'},
            {value: 'DataSource', label: 'Data source'}
        ];

        $scope.cacheStoreJdbcDialects = $common.cacheStoreJdbcDialects;

        $scope.toggleExpanded = function () {
            $scope.ui.expanded = !$scope.ui.expanded;

            $common.hidePopover();
        };

        $scope.general = [];
        $scope.advanced = [];
        $scope.caches = [];
        $scope.domains = [];

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

        function _cacheLbl(cache) {
            return cache.name + ', ' + cache.cacheMode + ', ' + cache.atomicityMode;
        }

        $scope.required = function (field) {
            var backupItem = $scope.backupItem;

            if (backupItem) {
                var model = $common.isDefined(field.path) ? field.path + '.' + field.model : field.model;

                var memoryMode = backupItem.memoryMode;

                var onHeapTired = memoryMode === 'ONHEAP_TIERED';
                var offHeapTired = memoryMode === 'OFFHEAP_TIERED';

                var offHeapMaxMemory = backupItem.offHeapMaxMemory;

                if (model === 'offHeapMaxMemory' && offHeapTired)
                    return true;

                if (model === 'evictionPolicy.kind' && onHeapTired)
                    return backupItem.swapEnabled || ($common.isDefined(offHeapMaxMemory) && offHeapMaxMemory >= 0);
            }

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
                        model.forEach(function (val, ix) {
                            if (ix !== index && val.split('=')[0] === key)
                                exist = true;
                        });
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
                        if (idx >= 0 && idx !== index)
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
                    return pair.keyClass === pairValue.key && pair.valueClass === pairValue.value;
                });

                // Found duplicate.
                if (idx >= 0 && idx !== index)
                    return showPopoverMessage(null, null, $table.tableFieldId(index, 'ValueIndexedType'), 'Indexed type with such key and value classes already exists!');
            }

            return true;
        };

        function selectFirstItem() {
            if ($scope.caches.length > 0)
                $scope.selectItem($scope.caches[0]);
        }

        function cacheDomains(item) {
            return _.reduce($scope.domains, function (memo, domain) {
                if (item && _.contains(item.domains, domain.value)) {
                    memo.push(domain.meta);
                }

                return memo;
            }, []);
        }

        $loading.start('loadingCachesScreen');

        // When landing on the page, get caches and show them.
        $http.post('/api/v1/configuration/caches/list')
            .success(function (data) {
                var validFilter = $filter('domainsValidation');

                $scope.spaces = data.spaces;

                data.caches.forEach(function (cache) {
                    cache.label = _cacheLbl(cache);
                });

                $scope.caches = data.caches;
                $scope.clusters = _.map(data.clusters, function (cluster) {
                    return {
                        value: cluster._id,
                        label: cluster.name,
                        caches: cluster.caches
                    };
                });
                $scope.domains = _.sortBy(_.map(validFilter(data.domains, true, false), function (domain) {
                    return {
                        value: domain._id,
                        label: domain.valueType,
                        kind: domain.kind,
                        meta: domain
                    };
                }), 'label');

                // Load page descriptor.
                $http.get('/models/caches.json')
                    .success(function (data) {
                        $scope.general = data.general;
                        $scope.advanced = data.advanced;

                        $scope.ui.addGroups(data.general, data.advanced);

                        if ($state.params.id)
                            $scope.createItem($state.params.id);
                        else {
                            var lastSelectedCache = angular.fromJson(sessionStorage.lastSelectedCache);

                            if (lastSelectedCache) {
                                var idx = _.findIndex($scope.caches, function (cache) {
                                    return cache._id === lastSelectedCache;
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

                                var domains = cacheDomains(val);
                                var varName = $commonUtils.toJavaName('cache', val.name);

                                $scope.preview.general.xml = $generatorXml.cacheDomains(domains, $generatorXml.cacheGeneral(val)).asString();
                                $scope.preview.general.java = $generatorJava.cacheDomains(domains, varName, $generatorJava.cacheGeneral(val, varName)).asString();
                                $scope.preview.general.allDefaults = $common.isEmptyString($scope.preview.general.xml);

                                $scope.preview.memory.xml = $generatorXml.cacheMemory(val).asString();
                                $scope.preview.memory.java = $generatorJava.cacheMemory(val, varName).asString();
                                $scope.preview.memory.allDefaults = $common.isEmptyString($scope.preview.memory.xml);

                                $scope.preview.query.xml = $generatorXml.cacheQuery(val).asString();
                                $scope.preview.query.java = $generatorJava.cacheQuery(val, varName).asString();
                                $scope.preview.query.allDefaults = $common.isEmptyString($scope.preview.query.xml);

                                var storeFactory = $generatorXml.cacheStore(val, domains);

                                $scope.preview.store.xml = $generatorXml.generateDataSources(storeFactory.datasources).asString() + storeFactory.asString();
                                $scope.preview.store.java = $generatorJava.cacheStore(val, domains, varName).asString();
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

                $scope.selectedItemWatchGuard = true;
                $scope.selectedItem = angular.copy(item);

                try {
                    if (item)
                        sessionStorage.lastSelectedCache = angular.toJson(item._id);
                    else
                        sessionStorage.removeItem('lastSelectedCache');
                }
                catch (error) { }

                _.forEach(previews, function (preview) {
                    preview.attractAttention = false;
                });

                if (backup)
                    $scope.backupItem = backup;
                else if (item)
                    $scope.backupItem = angular.copy(item);
                else
                    $scope.backupItem = undefined;

                if ($common.getQueryVariable('new'))
                    $state.go('base.configuration.caches');
            }

            $common.confirmUnsavedChanges($scope.ui.isDirty(), selectItem);
        };

        function prepareNewItem(id) {
            return {
                space: $scope.spaces[0]._id,
                cacheMode: 'PARTITIONED',
                atomicityMode: 'ATOMIC',
                readFromBackup: true,
                copyOnRead: true,
                clusters: id && _.find($scope.clusters, {value: id})
                    ? [id] : _.map($scope.clusters, function (cluster) { return cluster.value; }),
                domains: id && _.find($scope.domains, { value: id }) ? [id] : [],
                cacheStoreFactory: {CacheJdbcBlobStoreFactory: {connectVia: 'DataSource'}}
            };
        }

        // Add new cache.
        $scope.createItem = function (id) {
            if ($scope.tableReset(true)) {
                $timeout(function () {
                    $common.ensureActivePanel($scope.ui, 'general', 'cacheName');
                });

                $scope.selectItem(undefined, prepareNewItem(id));
            }
        };

        function checkDataSources() {
            var clusters = _.filter($scope.clusters, function (cluster) {
                return _.contains($scope.backupItem.clusters, cluster.value);
            });

            var checkRes = { checked: true };

            var failCluster = _.find(clusters, function (cluster) {
                var caches = _.filter($scope.caches, function (cache) {
                    return cache._id !== $scope.backupItem._id && _.find(cluster.caches, function (clusterCache) {
                        return clusterCache === cache._id;
                    });
                });

                caches.push($scope.backupItem);

                checkRes = $common.checkCachesDataSources(caches, $scope.backupItem);

                return !checkRes.checked;
            });

            if (!checkRes.checked) {
                return showPopoverMessage($scope.ui, 'store', checkRes.firstCache.cacheStoreFactory.kind === 'CacheJdbcPojoStoreFactory' ? 'pojoDialect' : 'blobDialect',
                    'Found cache "' + checkRes.secondCache.name + '" in cluster "' + failCluster.label + '" ' +
                    'with the same data source bean name "' + checkRes.firstCache.cacheStoreFactory[checkRes.firstCache.cacheStoreFactory.kind].dataSourceBean +
                    '" and different database: "' + $common.cacheStoreJdbcDialectsLabel(checkRes.firstDB) + '" in current cache and "' +
                    $common.cacheStoreJdbcDialectsLabel(checkRes.secondDB) + '" in "' + checkRes.secondCache.name + '"', 10000);
            }

            return true;
        }

        // Check cache logical consistency.
        function validate(item) {
            if ($common.isEmptyString(item.name))
                return showPopoverMessage($scope.ui, 'general', 'cacheName', 'Name should not be empty');

            if (item.memoryMode === 'OFFHEAP_VALUES' && !$common.isEmptyArray(item.domains))
                return showPopoverMessage($scope.ui, 'memory', 'memoryMode',
                    'Cannot have query indexing enabled while values are stored off-heap');

            if (item.memoryMode === 'OFFHEAP_TIERED' && !$common.isDefined(item.offHeapMaxMemory))
                return showPopoverMessage($scope.ui, 'memory', 'offHeapMaxMemory',
                    'Off-heap max memory should be specified');

            if (item.memoryMode === 'ONHEAP_TIERED' && item.offHeapMaxMemory > 0 && !$common.isDefined(item.evictionPolicy.kind))
                return showPopoverMessage($scope.ui, 'memory', 'evictionPolicy', 'Eviction policy should not be configured');

            var cacheStoreFactorySelected = item.cacheStoreFactory && item.cacheStoreFactory.kind;

            if (cacheStoreFactorySelected) {
                var storeFactory = item.cacheStoreFactory[item.cacheStoreFactory.kind];

                if (item.cacheStoreFactory.kind === 'CacheJdbcPojoStoreFactory') {
                    if ($common.isEmptyString(storeFactory.dataSourceBean))
                        return showPopoverMessage($scope.ui, 'store', 'dataSourceBean',
                            'Data source bean name should not be empty');

                    if (!$common.isValidJavaIdentifier('Data source bean', storeFactory.dataSourceBean, 'dataSourceBean', $scope.ui, 'store'))
                        return false;

                    if (!storeFactory.dialect)
                        return showPopoverMessage($scope.ui, 'store', 'pojoDialect',
                            'Dialect should not be empty');

                    if (!checkDataSources())
                        return false;
                }

                if (item.cacheStoreFactory.kind === 'CacheJdbcBlobStoreFactory') {
                    if (storeFactory.connectVia === 'URL') {
                        if ($common.isEmptyString(storeFactory.connectionUrl))
                            return showPopoverMessage($scope.ui, 'store', 'connectionUrl',
                                'Connection URL should not be empty');

                        if ($common.isEmptyString(storeFactory.user))
                            return showPopoverMessage($scope.ui, 'store', 'user',
                                'User should not be empty');
                    }
                    else {
                        if ($common.isEmptyString(storeFactory.dataSourceBean))
                            return showPopoverMessage($scope.ui, 'store', 'dataSourceBean',
                                'Data source bean name should not be empty');

                        if (!$common.isValidJavaIdentifier('Data source bean', storeFactory.dataSourceBean, 'dataSourceBean', $scope.ui, 'store'))
                            return false;

                        if (!storeFactory.dialect)
                            return showPopoverMessage($scope.ui, 'store', 'blobDialect',
                                'Database should not be empty');

                        if (!checkDataSources())
                            return false;
                    }
                }
            }

            if ((item.readThrough || item.writeThrough) && !cacheStoreFactorySelected)
                return showPopoverMessage($scope.ui, 'store', 'cacheStoreFactory',
                    (item.readThrough ? 'Read' : 'Write') + ' through are enabled but store is not configured!');

            if (item.writeBehindEnabled && !cacheStoreFactorySelected)
                return showPopoverMessage($scope.ui, 'store', 'cacheStoreFactory',
                    'Write behind enabled but store is not configured!');

            if (cacheStoreFactorySelected) {
                if (!item.readThrough && !item.writeThrough)
                    return showPopoverMessage($scope.ui, 'store', 'readThrough',
                        'Store is configured but read/write through are not enabled!');
            }

            if (item.writeBehindFlushSize === 0 && item.writeBehindFlushFrequency === 0)
                return showPopoverMessage($scope.ui, 'store', 'writeBehindFlushSize',
                    'Both "Flush frequency" and "Flush size" are not allowed as 0');

            if (item.cacheMode !== 'LOCAL' && item.rebalanceMode !== 'NONE' && item.rebalanceBatchSize === 0)
                return showPopoverMessage($scope.ui, 'rebalance', 'rebalanceBatchSize',
                    'Batch size should be more than 0 for not "NONE" rebalance mode', 10000);

            return true;
        }

        // Save cache into database.
        function save(item) {
            $http.post('/api/v1/configuration/caches/save', item)
                .success(function (_id) {
                    $scope.ui.markPristine();

                    item.label = _cacheLbl(item);

                    var idx = _.findIndex($scope.caches, function (cache) {
                        return cache._id === _id;
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
            if ($scope.tableReset(true)) {
                var item = $scope.backupItem;

                angular.extend(item, $common.autoCacheStoreConfiguration(item, cacheDomains(item)));

                if (validate(item))
                    save(item);
            }
        };

        function _cacheNames() {
            return _.map($scope.caches, function (cache) {
                return cache.name;
            });
        }

        // Save cache with new name.
        $scope.cloneItem = function () {
            if ($scope.tableReset(true) && validate($scope.backupItem)) {
                $clone.confirm($scope.backupItem.name, _cacheNames()).then(function (newName) {
                    var item = angular.copy($scope.backupItem);

                    delete item._id;
                    delete item.demo;

                    item.name = newName;

                    save(item);
                });
            }
        };

        // Remove cache from db.
        $scope.removeItem = function () {
            $table.tableReset();

            var selectedItem = $scope.selectedItem;

            $confirm.confirm('Are you sure you want to remove cache: "' + selectedItem.name + '"?')
                .then(function () {
                    var _id = selectedItem._id;

                    $http.post('/api/v1/configuration/caches/remove', {_id: _id})
                        .success(function () {
                            $common.showInfo('Cache has been removed: ' + selectedItem.name);

                            var caches = $scope.caches;

                            var idx = _.findIndex(caches, function (cache) {
                                return cache._id === _id;
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
                    $http.post('/api/v1/configuration/caches/remove/all')
                        .success(function () {
                            $common.showInfo('All caches have been removed');

                            $scope.caches = [];
                            $scope.ui.markPristine();
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

        $scope.resetAll = function () {
            $table.tableReset();

            $confirm.confirm('Are you sure you want to undo all changes for current cache?')
                .then(function () {
                    $scope.backupItem = $scope.selectedItem ? angular.copy($scope.selectedItem) : prepareNewItem();
                    $scope.ui.markPristine();
                });
        };
    }]
);
