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
import consoleModule from 'controllers/common-module';

consoleModule.controller('cachesController', [
    '$scope', '$http', '$state', '$filter', '$timeout', '$common', '$confirm', '$clone', '$loading', '$cleanup', '$unsavedChangesGuard',
    function($scope, $http, $state, $filter, $timeout, $common, $confirm, $clone, $loading, $cleanup, $unsavedChangesGuard) {
        $unsavedChangesGuard.install($scope);

        const emptyCache = {empty: true};

        let __original_value;

        const blank = {
            evictionPolicy: {},
            cacheStoreFactory: {},
            nearConfiguration: {}
        };

        // We need to initialize backupItem with empty object in order to properly used from angular directives.
        $scope.backupItem = emptyCache;

        $scope.ui = $common.formUI();
        $scope.ui.activePanels = [0];
        $scope.ui.topPanels = [0, 1, 2, 3];

        $scope.hidePopover = $common.hidePopover;
        $scope.saveBtnTipText = $common.saveBtnTipText;
        $scope.widthIsSufficient = $common.widthIsSufficient;

        const showPopoverMessage = $common.showPopoverMessage;

        $scope.contentVisible = function() {
            const item = $scope.backupItem;

            return !item.empty && (!item._id || _.find($scope.displayedRows, {_id: item._id}));
        };

        $scope.toggleExpanded = function() {
            $scope.ui.expanded = !$scope.ui.expanded;

            $common.hidePopover();
        };

        $scope.caches = [];
        $scope.domains = [];

        function _cacheLbl(cache) {
            return cache.name + ', ' + cache.cacheMode + ', ' + cache.atomicityMode;
        }

        function selectFirstItem() {
            if ($scope.caches.length > 0)
                $scope.selectItem($scope.caches[0]);
        }

        function cacheDomains(item) {
            return _.reduce($scope.domains, function(memo, domain) {
                if (item && _.includes(item.domains, domain.value))
                    memo.push(domain.meta);

                return memo;
            }, []);
        }

        $loading.start('loadingCachesScreen');

        // When landing on the page, get caches and show them.
        $http.post('/api/v1/configuration/caches/list')
            .success(function(data) {
                const validFilter = $filter('domainsValidation');

                $scope.spaces = data.spaces;
                $scope.caches = data.caches;

                _.forEach($scope.caches, (cache) => cache.label = _cacheLbl(cache));

                $scope.clusters = _.map(data.clusters, function(cluster) {
                    return {
                        value: cluster._id,
                        label: cluster.name,
                        caches: cluster.caches
                    };
                });

                $scope.domains = _.sortBy(_.map(validFilter(data.domains, true, false), function(domain) {
                    return {
                        value: domain._id,
                        label: domain.valueType,
                        kind: domain.kind,
                        meta: domain
                    };
                }), 'label');

                if ($state.params.linkId)
                    $scope.createItem($state.params.linkId);
                else {
                    const lastSelectedCache = angular.fromJson(sessionStorage.lastSelectedCache);

                    if (lastSelectedCache) {
                        const idx = _.findIndex($scope.caches, function(cache) {
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

                $scope.$watch('ui.inputForm.$valid', function(valid) {
                    if (valid && _.isEqual(__original_value, $cleanup($scope.backupItem)))
                        $scope.ui.inputForm.$dirty = false;
                });

                $scope.$watch('backupItem', function(val) {
                    const form = $scope.ui.inputForm;

                    if (form.$pristine || (form.$valid && _.isEqual(__original_value, $cleanup(val))))
                        form.$setPristine();
                    else
                        form.$setDirty();
                }, true);
            })
            .catch(function(errMsg) {
                $common.showError(errMsg);
            })
            .finally(function() {
                $scope.ui.ready = true;
                $scope.ui.inputForm.$setPristine();
                $loading.finish('loadingCachesScreen');
            });

        $scope.selectItem = function(item, backup) {
            function selectItem() {
                $scope.selectedItem = item;

                try {
                    if (item && item._id)
                        sessionStorage.lastSelectedCache = angular.toJson(item._id);
                    else
                        sessionStorage.removeItem('lastSelectedCache');
                }
                catch (ignored) {
                    // No-op.
                }

                if (backup)
                    $scope.backupItem = backup;
                else if (item)
                    $scope.backupItem = angular.copy(item);
                else
                    $scope.backupItem = emptyCache;

                $scope.backupItem = angular.merge({}, blank, $scope.backupItem);

                __original_value = $cleanup($scope.backupItem);

                if ($common.getQueryVariable('new'))
                    $state.go('base.configuration.caches');
            }

            $common.confirmUnsavedChanges($scope.backupItem && $scope.ui.inputForm.$dirty, selectItem);
        };

        $scope.linkId = () => $scope.backupItem._id ? $scope.backupItem._id : 'create';

        function prepareNewItem(linkId) {
            return {
                space: $scope.spaces[0]._id,
                cacheMode: 'PARTITIONED',
                atomicityMode: 'ATOMIC',
                readFromBackup: true,
                copyOnRead: true,
                clusters: linkId && _.find($scope.clusters, {value: linkId})
                    ? [linkId] : _.map($scope.clusters, function(cluster) { return cluster.value; }),
                domains: linkId && _.find($scope.domains, { value: linkId }) ? [linkId] : [],
                cacheStoreFactory: {CacheJdbcBlobStoreFactory: {connectVia: 'DataSource'}}
            };
        }

        // Add new cache.
        $scope.createItem = function(linkId) {
            $timeout(() => $common.ensureActivePanel($scope.ui, 'general', 'cacheName'));

            $scope.selectItem(null, prepareNewItem(linkId));
        };

        function cacheClusters() {
            return _.filter($scope.clusters, (cluster) => _.includes($scope.backupItem.clusters, cluster.value));
        }

        function clusterCaches(cluster) {
            const caches = _.filter($scope.caches,
                (cache) => cache._id !== $scope.backupItem._id && _.includes(cluster.caches, cache._id));

            caches.push($scope.backupItem);

            return caches;
        }

        function checkDataSources() {
            const clusters = cacheClusters();

            let checkRes = {checked: true};

            const failCluster = _.find(clusters, (cluster) => {
                const caches = clusterCaches(cluster);

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

        function checkSQLSchemas() {
            const clusters = cacheClusters();

            let checkRes = {checked: true};

            const failCluster = _.find(clusters, (cluster) => {
                const caches = clusterCaches(cluster);

                checkRes = $common.checkCacheSQLSchemas(caches, $scope.backupItem);

                return !checkRes.checked;
            });

            if (!checkRes.checked) {
                return showPopoverMessage($scope.ui, 'query', 'sqlSchema',
                    'Found cache "' + checkRes.secondCache.name + '" in cluster "' + failCluster.label + '" ' +
                    'with the same SQL schema name "' + checkRes.firstCache.sqlSchema + '"', 10000);
            }

            return true;
        }

        function checkStoreFactoryBean(storeFactory, beanFieldId) {
            if (!$common.isValidJavaIdentifier('Data source bean', storeFactory.dataSourceBean, beanFieldId, $scope.ui, 'store'))
                return false;

            return checkDataSources();
        }

        function checkStoreFactory(item) {
            const cacheStoreFactorySelected = item.cacheStoreFactory && item.cacheStoreFactory.kind;

            if (cacheStoreFactorySelected) {
                const storeFactory = item.cacheStoreFactory[item.cacheStoreFactory.kind];

                if (item.cacheStoreFactory.kind === 'CacheJdbcPojoStoreFactory' && !checkStoreFactoryBean(storeFactory, 'pojoDataSourceBean'))
                    return false;

                if (item.cacheStoreFactory.kind === 'CacheJdbcBlobStoreFactory' && storeFactory.connectVia !== 'URL'
                    && !checkStoreFactoryBean(storeFactory, 'blobDataSourceBean'))
                    return false;
            }

            if ((item.readThrough || item.writeThrough) && !cacheStoreFactorySelected)
                return showPopoverMessage($scope.ui, 'store', 'cacheStoreFactory', (item.readThrough ? 'Read' : 'Write') + ' through are enabled but store is not configured!');

            if (item.writeBehindEnabled && !cacheStoreFactorySelected)
                return showPopoverMessage($scope.ui, 'store', 'cacheStoreFactory', 'Write behind enabled but store is not configured!');

            if (cacheStoreFactorySelected && !item.readThrough && !item.writeThrough)
                return showPopoverMessage($scope.ui, 'store', 'readThroughTooltip', 'Store is configured but read/write through are not enabled!');

            return true;
        }

        // Check cache logical consistency.
        function validate(item) {
            $common.hidePopover();

            if ($common.isEmptyString(item.name))
                return showPopoverMessage($scope.ui, 'general', 'cacheName', 'Cache name should not be empty!');

            if (item.memoryMode === 'ONHEAP_TIERED' && item.offHeapMaxMemory > 0 && !$common.isDefined(item.evictionPolicy.kind))
                return showPopoverMessage($scope.ui, 'memory', 'evictionPolicyKind', 'Eviction policy should not be configured!');

            if (!$common.checkFieldValidators($scope.ui))
                return false;

            if (item.memoryMode === 'OFFHEAP_VALUES' && !_.isEmpty(item.domains))
                return showPopoverMessage($scope.ui, 'memory', 'memoryMode', 'Query indexing could not be enabled while values are stored off-heap!');

            if (item.memoryMode === 'OFFHEAP_TIERED' && (!$common.isDefined(item.offHeapMaxMemory) || item.offHeapMaxMemory < 0))
                return showPopoverMessage($scope.ui, 'memory', 'offHeapMaxMemory', 'Off-heap max memory should be specified!');

            if (!checkSQLSchemas())
                return false;

            if (!checkStoreFactory(item))
                return false;

            if (item.writeBehindFlushSize === 0 && item.writeBehindFlushFrequency === 0)
                return showPopoverMessage($scope.ui, 'store', 'writeBehindFlushSize', 'Both "Flush frequency" and "Flush size" are not allowed as 0!');

            return true;
        }

        // Save cache in database.
        function save(item) {
            $http.post('/api/v1/configuration/caches/save', item)
                .success(function(_id) {
                    item.label = _cacheLbl(item);

                    $scope.ui.inputForm.$setPristine();

                    const idx = _.findIndex($scope.caches, function(cache) {
                        return cache._id === _id;
                    });

                    if (idx >= 0)
                        angular.merge($scope.caches[idx], item);
                    else {
                        item._id = _id;
                        $scope.caches.push(item);
                    }

                    _.forEach($scope.clusters, (cluster) => {
                        if (_.includes(item.clusters, cluster.value))
                            cluster.caches = _.union(cluster.caches, [_id]);
                        else
                            _.remove(cluster.caches, (id) => id === _id);
                    });

                    _.forEach($scope.domains, (domain) => {
                        if (_.includes(item.domains, domain.value))
                            domain.meta.caches = _.union(domain.meta.caches, [_id]);
                        else
                            _.remove(domain.meta.caches, (id) => id === _id);
                    });

                    $scope.selectItem(item);

                    $common.showInfo('Cache "' + item.name + '" saved.');
                })
                .error(function(errMsg) {
                    $common.showError(errMsg);
                });
        }

        // Save cache.
        $scope.saveItem = function() {
            const item = $scope.backupItem;

            angular.extend(item, $common.autoCacheStoreConfiguration(item, cacheDomains(item)));

            if (validate(item))
                save(item);
        };

        function _cacheNames() {
            return _.map($scope.caches, function(cache) {
                return cache.name;
            });
        }

        // Clone cache with new name.
        $scope.cloneItem = function() {
            if (validate($scope.backupItem)) {
                $clone.confirm($scope.backupItem.name, _cacheNames()).then(function(newName) {
                    const item = angular.copy($scope.backupItem);

                    delete item._id;

                    item.name = newName;

                    delete item.sqlSchema;

                    save(item);
                });
            }
        };

        // Remove cache from db.
        $scope.removeItem = function() {
            const selectedItem = $scope.selectedItem;

            $confirm.confirm('Are you sure you want to remove cache: "' + selectedItem.name + '"?')
                .then(function() {
                    const _id = selectedItem._id;

                    $http.post('/api/v1/configuration/caches/remove', {_id})
                        .success(function() {
                            $common.showInfo('Cache has been removed: ' + selectedItem.name);

                            const caches = $scope.caches;

                            const idx = _.findIndex(caches, function(cache) {
                                return cache._id === _id;
                            });

                            if (idx >= 0) {
                                caches.splice(idx, 1);

                                if (caches.length > 0)
                                    $scope.selectItem(caches[0]);
                                else {
                                    $scope.backupItem = emptyCache;
                                    $scope.ui.inputForm.$setPristine();
                                }

                                _.forEach($scope.clusters, (cluster) => _.remove(cluster.caches, (id) => id === _id));
                                _.forEach($scope.domains, (domain) => _.remove(domain.meta.caches, (id) => id === _id));
                            }
                        })
                        .error(function(errMsg) {
                            $common.showError(errMsg);
                        });
                });
        };

        // Remove all caches from db.
        $scope.removeAllItems = function() {
            $confirm.confirm('Are you sure you want to remove all caches?')
                .then(function() {
                    $http.post('/api/v1/configuration/caches/remove/all')
                        .success(function() {
                            $common.showInfo('All caches have been removed');

                            $scope.caches = [];

                            _.forEach($scope.clusters, (cluster) => cluster.caches = []);
                            _.forEach($scope.domains, (domain) => domain.meta.caches = []);

                            $scope.backupItem = emptyCache;
                            $scope.ui.inputForm.$setPristine();
                        })
                        .error(function(errMsg) {
                            $common.showError(errMsg);
                        });
                });
        };

        $scope.resetAll = function() {
            $confirm.confirm('Are you sure you want to undo all changes for current cache?')
                .then(function() {
                    $scope.backupItem = $scope.selectedItem ? angular.copy($scope.selectedItem) : prepareNewItem();
                    $scope.ui.inputForm.$setPristine();
                });
        };
    }]
);
