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

// Controller for Clusters screen.
consoleModule.controller('clustersController', function ($http, $timeout, $scope, $state, $controller,
    $common, $focus, $confirm, $clone, $loading, $unsavedChangesGuard, $cleanup, igniteEventGroups) {
        $unsavedChangesGuard.install($scope);

        var __original_value;

        var blank = {
            atomicConfiguration: {},
            binaryConfiguration: {},
            communication: {},
            connector: {},
            discovery: {},
            marshaller: {},
            sslContextFactory: {},
            swapSpaceSpi: {},
            transactionConfiguration: {}
        };

        // Initialize the super class and extend it.
        angular.extend(this, $controller('save-remove', {$scope: $scope}));

        $scope.ui = $common.formUI();
        $scope.ui.angularWay = true; // TODO We need to distinguish refactored UI from legacy UI.
        $scope.ui.activePanels = [0];
        $scope.ui.topPanels = [0];

        $scope.compactJavaName = $common.compactJavaName;
        $scope.widthIsSufficient = $common.widthIsSufficient;
        $scope.saveBtnTipText = $common.saveBtnTipText;

        $scope.contentVisible = function () {
            var item = $scope.backupItem;

            return item && (!item._id || _.find($scope.displayedRows, {_id: item._id}));
        };

        $scope.trustManagersConfigured = function() {
            return $scope.backupItem.sslEnabled && $common.isDefined($scope.backupItem.sslContextFactory) &&
                !$common.isEmptyArray($scope.backupItem.sslContextFactory.trustManagers);
        };

        $scope.hidePopover = $common.hidePopover;

        var showPopoverMessage = $common.showPopoverMessage;

        $scope.discoveries = [
            {value: 'Vm', label: 'static IPs'},
            {value: 'Multicast', label: 'multicast'},
            {value: 'S3', label: 'AWS S3'},
            {value: 'Cloud', label: 'apache jclouds'},
            {value: 'GoogleStorage', label: 'google cloud storage'},
            {value: 'Jdbc', label: 'JDBC'},
            {value: 'SharedFs', label: 'shared filesystem'}
        ];

        $scope.swapSpaceSpis = [
            {value: 'FileSwapSpaceSpi', label: 'File-based swap'},
            {value: undefined, label: 'Not set'}
        ];

        $scope.eventGroups = igniteEventGroups;

        $scope.toggleExpanded = function () {
            $scope.ui.expanded = !$scope.ui.expanded;

            $common.hidePopover();
        };

        $scope.clusters = [];

        function _clusterLbl (cluster) {
            return cluster.name + ', ' + _.find($scope.discoveries, {value: cluster.discovery.kind}).label;
        }

        function selectFirstItem() {
            if ($scope.clusters.length > 0)
                $scope.selectItem($scope.clusters[0]);
        }

        function clusterCaches(item) {
            return _.reduce($scope.caches, function (memo, cache) {
                if (item && _.contains(item.caches, cache.value)) {
                    memo.push(cache.cache);
                }

                return memo;
            }, []);
        }

        $loading.start('loadingClustersScreen');

        // When landing on the page, get clusters and show them.
        $http.post('/api/v1/configuration/clusters/list')
            .success(function (data) {
                $scope.spaces = data.spaces;

                data.clusters.forEach(function (cluster) {
                    cluster.label = _clusterLbl(cluster);
                });

                $scope.clusters = data.clusters;

                $scope.caches = _.map(data.caches, function (cache) {
                    return {value: cache._id, label: cache.name, cache: cache};
                });

                $scope.igfss = _.map(data.igfss, function (igfs) {
                    return {value: igfs._id, label: igfs.name, igfs: igfs};
                });

                // Load page descriptor.
                if ($state.params.id)
                    $scope.createItem($state.params.id);
                else {
                    var lastSelectedCluster = angular.fromJson(sessionStorage.lastSelectedCluster);

                    if (lastSelectedCluster) {
                        var idx = _.findIndex($scope.clusters, function (cluster) {
                            return cluster._id === lastSelectedCluster;
                        });

                        if (idx >= 0)
                            $scope.selectItem($scope.clusters[idx]);
                        else {
                            sessionStorage.removeItem('lastSelectedCluster');

                            selectFirstItem();
                        }
                    }
                    else
                        selectFirstItem();
                }

                $scope.$watch('ui.inputForm.$valid', function(valid) {
                    if (valid && __original_value === JSON.stringify($cleanup($scope.backupItem))) {
                        $scope.ui.inputForm.$dirty = false;
                    }
                });

                $scope.$watch('backupItem', function (val) {
                    var form = $scope.ui.inputForm;

                    if (form.$pristine || (form.$valid && __original_value === JSON.stringify($cleanup(val))))
                        form.$setPristine();
                    else
                        form.$setDirty();
                }, true);
            })
            .catch(function (errMsg) {
                $common.showError(errMsg);
            })
            .finally(function () {
                $scope.ui.ready = true;
                $scope.ui.inputForm.$setPristine();
                $loading.finish('loadingClustersScreen');
            });

        $scope.selectItem = function (item, backup) {
            function selectItem() {
                $scope.selectedItem = item;

                try {
                    if (item && item._id)
                        sessionStorage.lastSelectedCluster = angular.toJson(item._id);
                    else
                        sessionStorage.removeItem('lastSelectedCluster');
                }
                catch (error) { }

                if (backup)
                    $scope.backupItem = backup;
                else if (item)
                    $scope.backupItem = angular.copy(item);
                else
                    $scope.backupItem = undefined;

                $scope.backupItem = angular.merge({}, blank, $scope.backupItem);

                __original_value = JSON.stringify($cleanup($scope.backupItem));

                if ($common.getQueryVariable('new'))
                    $state.go('base.configuration.clusters');
            }

            $common.confirmUnsavedChanges($scope.backupItem && $scope.ui.inputForm.$dirty, selectItem);
        };

        function prepareNewItem(id) {
            var newItem = {
                discovery: {kind: 'Multicast', Vm: {addresses: ['127.0.0.1:47500..47510']}, Multicast: {}},
                binaryConfiguration: {
                    typeConfigurations: [],
                    compactFooter: true
                },
                communication: {
                    tcpNoDelay: true
                },
                connector: {
                    noDelay: true
                }
            };

            newItem = angular.merge({}, blank, newItem);

            newItem.caches = id && _.find($scope.caches, {value: id}) ? [id] : [];
            newItem.igfss = id && _.find($scope.igfss, {value: id}) ? [id] : [];
            newItem.space = $scope.spaces[0]._id;

            return newItem;
        }

        // Add new cluster.
        $scope.createItem = function(id) {
            $timeout(function () {
                $common.ensureActivePanel($scope.ui, "general", 'clusterName');
            });

            $scope.selectItem(undefined, prepareNewItem(id));
        };

        $scope.indexOfCache = function (cacheId) {
            return _.findIndex($scope.caches, function (cache) {
                return cache.value === cacheId;
            });
        };

        // Check cluster logical consistency.
        function validate(item) {
            $common.hidePopover();

            if ($common.isEmptyString(item.name))
                return showPopoverMessage($scope.ui, 'general', 'clusterName', 'Name should not be empty');

            var form = $scope.ui.inputForm;
            var errors = form.$error;
            var errKeys = Object.keys(errors);

            if (errKeys && errKeys.length > 0) {
                var firstErrorKey = errKeys[0];

                var firstError = errors[firstErrorKey][0];
                var actualError = firstError.$error[firstErrorKey][0];

                var msg = 'Invalid value';

                try {
                    msg = form[firstError.$name].$errorMessages[actualError.$name][firstErrorKey];
                }
                catch(ignored) {
                    msg = 'Invalid value';
                }

                return showPopoverMessage($scope.ui, firstError.$name, actualError.$name, msg);
            }

            var caches = _.filter(_.map($scope.caches, function (scopeCache) {
                return scopeCache.cache;
            }), function (cache) {
                return _.contains($scope.backupItem.caches, cache._id);
            });

            var checkRes = $common.checkCachesDataSources(caches);

            if (!checkRes.checked) {
                return showPopoverMessage($scope.ui, 'general', 'caches',
                    'Found caches "' + checkRes.firstCache.name + '" and "' + checkRes.secondCache.name + '" ' +
                    'with the same data source bean name "' + checkRes.firstCache.cacheStoreFactory[checkRes.firstCache.cacheStoreFactory.kind].dataSourceBean +
                    '" and different databases: "' + $common.cacheStoreJdbcDialectsLabel(checkRes.firstDB) + '" in "' + checkRes.firstCache.name + '" and "' +
                    $common.cacheStoreJdbcDialectsLabel(checkRes.secondDB) + '" in "' + checkRes.secondCache.name + '"', 10000);
            }

            var b = item.binaryConfiguration;

            if ($common.isDefined(b)) {
                if (!$common.isEmptyArray(b.typeConfigurations)) {
                    var sameName = function (t, ix) {
                        return ix < typeIx && t.typeName === type.typeName;
                    };

                    for (var typeIx = 0; typeIx < b.typeConfigurations.length; typeIx++) {
                        var type = b.typeConfigurations[typeIx];

                        if ($common.isEmptyString(type.typeName))
                            return showPopoverMessage($scope.ui, 'binary', 'typeName' + typeIx, 'Type name should be specified');

                        if (_.find(b.typeConfigurations, sameName))
                            return showPopoverMessage($scope.ui, 'binary', 'typeName' + typeIx, 'Type with such name is already specified');
                    }
                }
            }

            var c = item.communication;

            if ($common.isDefined(c)) {
                if ($common.isDefined(c.unacknowledgedMessagesBufferSize)) {
                    if ($common.isDefined(c.messageQueueLimit))
                        if (c.unacknowledgedMessagesBufferSize < 5 * c.messageQueueLimit)
                            return showPopoverMessage($scope.ui, 'communication', 'unacknowledgedMessagesBufferSize', 'Maximum number of stored unacknowledged messages should be at least 5 * message queue limit');

                    if ($common.isDefined(c.ackSendThreshold))
                        if (c.unacknowledgedMessagesBufferSize < 5 * c.ackSendThreshold)
                            return showPopoverMessage($scope.ui, 'communication', 'unacknowledgedMessagesBufferSize', 'Maximum number of stored unacknowledged messages should be at least 5 * ack send threshold');
                }

                if (c.sharedMemoryPort === 0)
                    return showPopoverMessage($scope.ui, 'communication', 'sharedMemoryPort', 'Shared memory port should be more than 0 or -1');
            }

            var r = item.connector;

            if ($common.isDefined(r)) {
                if (r.sslEnabled && $common.isEmptyString(r.sslFactory))
                    return showPopoverMessage($scope.ui, 'connector', 'connectorSslFactory', 'SSL factory should not be empty');
            }

            var d = item.discovery;

            if (d) {
                if ((d.maxAckTimeout != undefined ? d.maxAckTimeout : 600000) < (d.ackTimeout || 5000))
                    return showPopoverMessage($scope.ui, 'discovery', 'ackTimeout', 'Acknowledgement timeout should be less than max acknowledgement timeout');

                if (d.kind === 'Vm' && d.Vm && d.Vm.addresses.length === 0)
                    return showPopoverMessage($scope.ui, 'general', 'addresses', 'Addresses are not specified');

                if (d.kind === 'S3' && d.S3 && $common.isEmptyString(d.S3.bucketName))
                    return showPopoverMessage($scope.ui, 'general', 'bucketName', 'Bucket name should not be empty');

                if (d.kind === 'Cloud' && d.Cloud) {
                    if ($common.isEmptyString(d.Cloud.identity))
                        return showPopoverMessage($scope.ui, 'general', 'identity', 'Identity should not be empty');

                    if ($common.isEmptyString(d.Cloud.provider))
                        return showPopoverMessage($scope.ui, 'general', 'provider', 'Provider should not be empty');
                }

                if (d.kind === 'GoogleStorage' && d.GoogleStorage) {
                    if ($common.isEmptyString(d.GoogleStorage.projectName))
                        return showPopoverMessage($scope.ui, 'general', 'projectName', 'Project name should not be empty');

                    if ($common.isEmptyString(d.GoogleStorage.bucketName))
                        return showPopoverMessage($scope.ui, 'general', 'bucketName', 'Bucket name should not be empty');

                    if ($common.isEmptyString(d.GoogleStorage.serviceAccountP12FilePath))
                        return showPopoverMessage($scope.ui, 'general', 'serviceAccountP12FilePath', 'Private key path should not be empty');

                    if ($common.isEmptyString(d.GoogleStorage.serviceAccountId))
                        return showPopoverMessage($scope.ui, 'general', 'serviceAccountId', 'Account ID should not be empty');
                }
            }

            var swapKind = item.swapSpaceSpi && item.swapSpaceSpi.kind;

            if (swapKind && item.swapSpaceSpi[swapKind]) {
                var swap = item.swapSpaceSpi[swapKind];

                var sparsity = swap.maximumSparsity;

                if ($common.isDefined(sparsity) && (sparsity < 0 || sparsity >= 1))
                    return showPopoverMessage($scope.ui, 'swap', 'maximumSparsity', 'Maximum sparsity should be more or equal 0 and less than 1');

                var readStripesNumber = swap.readStripesNumber;

                if (readStripesNumber && !(readStripesNumber == -1 || (readStripesNumber & (readStripesNumber - 1)) == 0))
                    return showPopoverMessage($scope.ui, 'swap', 'readStripesNumber', 'Read stripe size must be positive and power of two');
            }

            if (item.sslEnabled) {
                if (!$common.isDefined(item.sslContextFactory) || $common.isEmptyString(item.sslContextFactory.keyStoreFilePath))
                    return showPopoverMessage($scope.ui, 'sslConfiguration', 'keyStoreFilePath', 'Key store file should not be empty');

                if ($common.isEmptyString(item.sslContextFactory.trustStoreFilePath) && $common.isEmptyArray(item.sslContextFactory.trustManagers))
                    return showPopoverMessage($scope.ui, 'sslConfiguration', 'sslConfiguration-title', 'Trust storage file or managers should be configured');
            }

            if (!swapKind && item.caches) {
                for (var i = 0; i < item.caches.length; i++) {
                    var idx = $scope.indexOfCache(item.caches[i]);

                    if (idx >= 0) {
                        var cache = $scope.caches[idx];

                        if (cache.cache.swapEnabled)
                            return showPopoverMessage($scope.ui, 'swap', 'swapSpaceSpi',
                                'Swap space SPI is not configured, but cache "' + cache.label + '" configured to use swap!');
                    }
                }
            }

            if (item.rebalanceThreadPoolSize && item.systemThreadPoolSize && item.systemThreadPoolSize <= item.rebalanceThreadPoolSize)
                return showPopoverMessage($scope.ui, 'pools', 'rebalanceThreadPoolSize',
                    'Rebalance thread pool size exceed or equals System thread pool size');

            return true;
        }

        // Save cluster in database.
        function save(item) {
            $http.post('/api/v1/configuration/clusters/save', item)
                .success(function (_id) {
                    item.label = _clusterLbl(item);

                    $scope.ui.inputForm.$setPristine();

                    var idx = _.findIndex($scope.clusters, function (cluster) {
                        return cluster._id === _id;
                    });

                    if (idx >= 0)
                        angular.merge($scope.clusters[idx], item);
                    else {
                        item._id = _id;
                        $scope.clusters.push(item);
                    }

                    $scope.selectItem(item);

                    $common.showInfo('Cluster "' + item.name + '" saved.');
                })
                .error(function (errMsg) {
                    $common.showError(errMsg);
                });
        }

        // Save cluster.
        $scope.saveItem = function () {
            var item = $scope.backupItem;

            var swapSpi = $common.autoClusterSwapSpiConfiguration(item, clusterCaches(item));

            if (swapSpi)
                angular.extend(item, swapSpi);

            if (validate(item))
                save(item);
        };

        function _clusterNames() {
            return _.map($scope.clusters, function (cluster) {
                return cluster.name;
            });
        }

        // Copy cluster with new name.
        $scope.cloneItem = function () {
            if (validate($scope.backupItem)) {
                $clone.confirm($scope.backupItem.name, _clusterNames()).then(function (newName) {
                    var item = angular.copy($scope.backupItem);

                    delete item._id;
                    item.name = newName;

                    save(item);
                });
            }
        };

        // Remove cluster from db.
        $scope.removeItem = function () {
            var selectedItem = $scope.selectedItem;

            $confirm.confirm('Are you sure you want to remove cluster: "' + selectedItem.name + '"?')
                .then(function () {
                        var _id = selectedItem._id;

                        $http.post('/api/v1/configuration/clusters/remove', {_id: _id})
                            .success(function () {
                                $common.showInfo('Cluster has been removed: ' + selectedItem.name);

                                var clusters = $scope.clusters;

                                var idx = _.findIndex(clusters, function (cluster) {
                                    return cluster._id === _id;
                                });

                                if (idx >= 0) {
                                    clusters.splice(idx, 1);

                                    if (clusters.length > 0)
                                        $scope.selectItem(clusters[0]);
                                    else
                                        $scope.backupItem = undefined;
                                }
                            })
                            .error(function (errMsg) {
                                $common.showError(errMsg);
                            });
                });
        };

        // Remove all clusters from db.
        $scope.removeAllItems = function () {
            $confirm.confirm('Are you sure you want to remove all clusters?')
                .then(function () {
                        $http.post('/api/v1/configuration/clusters/remove/all')
                            .success(function () {
                                $common.showInfo('All clusters have been removed');

                                $scope.clusters = [];
                                $scope.backupItem = undefined;
                                $scope.ui.inputForm.$setPristine();
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
            $confirm.confirm('Are you sure you want to undo all changes for current cluster?')
                .then(function() {
                    $scope.backupItem = $scope.selectedItem ? angular.copy($scope.selectedItem) : prepareNewItem();
                    $scope.ui.inputForm.$setPristine();
                });
        };
    }
);
