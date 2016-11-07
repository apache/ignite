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
export default ['clustersController', [
    '$rootScope', '$scope', '$http', '$state', '$timeout', 'IgniteLegacyUtils', 'IgniteMessages', 'IgniteConfirm', 'IgniteClone', 'IgniteLoading', 'IgniteModelNormalizer', 'IgniteUnsavedChangesGuard', 'igniteEventGroups', 'DemoInfo', 'IgniteLegacyTable', 'igniteConfigurationResource', 'IgniteErrorPopover', 'IgniteFormUtils',
    function($root, $scope, $http, $state, $timeout, LegacyUtils, Messages, Confirm, Clone, Loading, ModelNormalizer, UnsavedChangesGuard, igniteEventGroups, DemoInfo, LegacyTable, Resource, ErrorPopover, FormUtils) {
        UnsavedChangesGuard.install($scope);

        const emptyCluster = {empty: true};

        let __original_value;

        const blank = {
            atomicConfiguration: {},
            binaryConfiguration: {},
            cacheKeyConfiguration: [],
            communication: {},
            connector: {},
            discovery: {
                Cloud: {
                    regions: [],
                    zones: []
                }
            },
            marshaller: {},
            sslContextFactory: {
                trustManagers: []
            },
            swapSpaceSpi: {},
            transactionConfiguration: {},
            collision: {}
        };

        const pairFields = {
            attributes: {id: 'Attribute', idPrefix: 'Key', searchCol: 'name', valueCol: 'key', dupObjName: 'name', group: 'attributes'},
            'collision.JobStealing.stealingAttributes': {id: 'CAttribute', idPrefix: 'Key', searchCol: 'name', valueCol: 'key', dupObjName: 'name', group: 'collision'}
        };

        $scope.tablePairValid = function(item, field, index, stopEdit) {
            const pairField = pairFields[field.model];

            const pairValue = LegacyTable.tablePairValue(field, index);

            if (pairField) {
                const model = _.get(item, field.model);

                if (LegacyUtils.isDefined(model)) {
                    const idx = _.findIndex(model, (pair) => {
                        return pair[pairField.searchCol] === pairValue[pairField.valueCol];
                    });

                    // Found duplicate by key.
                    if (idx >= 0 && idx !== index) {
                        if (stopEdit)
                            return false;

                        return ErrorPopover.show(LegacyTable.tableFieldId(index, pairField.idPrefix + pairField.id), 'Attribute with such ' + pairField.dupObjName + ' already exists!', $scope.ui, pairField.group);
                    }
                }
            }

            return true;
        };

        $scope.tableSave = function(field, index, stopEdit) {
            if (LegacyTable.tablePairSaveVisible(field, index))
                return LegacyTable.tablePairSave($scope.tablePairValid, $scope.backupItem, field, index, stopEdit);

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
            if ($scope.tableReset(true)) {
                if (field.type === 'failoverSpi') {
                    if (LegacyUtils.isDefined($scope.backupItem.failoverSpi))
                        $scope.backupItem.failoverSpi.push({});
                    else
                        $scope.backupItem.failoverSpi = {};
                }
                else
                    LegacyTable.tableNewItem(field);
            }
        };

        $scope.tableNewItemActive = LegacyTable.tableNewItemActive;

        $scope.tableStartEdit = function(item, field, index) {
            if ($scope.tableReset(true))
                LegacyTable.tableStartEdit(item, field, index, $scope.tableSave);
        };

        $scope.tableEditing = LegacyTable.tableEditing;

        $scope.tableRemove = function(item, field, index) {
            if ($scope.tableReset(true))
                LegacyTable.tableRemove(item, field, index);
        };

        $scope.tablePairSave = LegacyTable.tablePairSave;
        $scope.tablePairSaveVisible = LegacyTable.tablePairSaveVisible;

        $scope.attributesTbl = {
            type: 'attributes',
            model: 'attributes',
            focusId: 'Attribute',
            ui: 'table-pair',
            keyName: 'name',
            valueName: 'value',
            save: $scope.tableSave
        };

        $scope.stealingAttributesTbl = {
            type: 'attributes',
            model: 'collision.JobStealing.stealingAttributes',
            focusId: 'CAttribute',
            ui: 'table-pair',
            keyName: 'name',
            valueName: 'value',
            save: $scope.tableSave
        };

        $scope.removeFailoverConfiguration = function(idx) {
            $scope.backupItem.failoverSpi.splice(idx, 1);
        };

        // We need to initialize backupItem with empty object in order to properly used from angular directives.
        $scope.backupItem = emptyCluster;

        $scope.ui = FormUtils.formUI();
        $scope.ui.activePanels = [0];
        $scope.ui.topPanels = [0];

        $scope.saveBtnTipText = FormUtils.saveBtnTipText;
        $scope.widthIsSufficient = FormUtils.widthIsSufficient;

        $scope.contentVisible = function() {
            const item = $scope.backupItem;

            return !item.empty && (!item._id || _.find($scope.displayedRows, {_id: item._id}));
        };

        $scope.toggleExpanded = function() {
            $scope.ui.expanded = !$scope.ui.expanded;

            ErrorPopover.hide();
        };

        $scope.discoveries = [
            {value: 'Vm', label: 'Static IPs'},
            {value: 'Multicast', label: 'Multicast'},
            {value: 'S3', label: 'AWS S3'},
            {value: 'Cloud', label: 'Apache jclouds'},
            {value: 'GoogleStorage', label: 'Google cloud storage'},
            {value: 'Jdbc', label: 'JDBC'},
            {value: 'SharedFs', label: 'Shared filesystem'},
            {value: 'ZooKeeper', label: 'Apache ZooKeeper'}
        ];

        $scope.swapSpaceSpis = [
            {value: 'FileSwapSpaceSpi', label: 'File-based swap'},
            {value: null, label: 'Not set'}
        ];

        $scope.eventGroups = igniteEventGroups;

        $scope.clusters = [];

        function _clusterLbl(cluster) {
            return cluster.name + ', ' + _.find($scope.discoveries, {value: cluster.discovery.kind}).label;
        }

        function selectFirstItem() {
            if ($scope.clusters.length > 0)
                $scope.selectItem($scope.clusters[0]);
        }

        Loading.start('loadingClustersScreen');

        // When landing on the page, get clusters and show them.
        Resource.read()
            .then(({spaces, clusters, caches, igfss}) => {
                $scope.spaces = spaces;
                $scope.clusters = clusters;

                $scope.caches = _.map(caches, (cache) => ({value: cache._id, label: cache.name, cache}));
                $scope.igfss = _.map(igfss, (igfs) => ({value: igfs._id, label: igfs.name, igfs}));

                _.forEach($scope.clusters, (cluster) => {
                    cluster.label = _clusterLbl(cluster);

                    if (!cluster.collision || !cluster.collision.kind)
                        cluster.collision = {kind: 'Noop', JobStealing: {stealingEnabled: true}, PriorityQueue: {starvationPreventionEnabled: true}};

                    if (!cluster.failoverSpi)
                        cluster.failoverSpi = [];

                    if (!cluster.logger)
                        cluster.logger = {Log4j: { mode: 'Default'}};
                });

                if ($state.params.linkId)
                    $scope.createItem($state.params.linkId);
                else {
                    const lastSelectedCluster = angular.fromJson(sessionStorage.lastSelectedCluster);

                    if (lastSelectedCluster) {
                        const idx = _.findIndex($scope.clusters, (cluster) => cluster._id === lastSelectedCluster);

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
                    if (valid && ModelNormalizer.isEqual(__original_value, $scope.backupItem))
                        $scope.ui.inputForm.$dirty = false;
                });

                $scope.$watch('backupItem', function(val) {
                    if (!$scope.ui.inputForm)
                        return;

                    const form = $scope.ui.inputForm;

                    if (form.$valid && ModelNormalizer.isEqual(__original_value, val))
                        form.$setPristine();
                    else
                        form.$setDirty();
                }, true);

                $scope.$watch('ui.activePanels.length', () => {
                    ErrorPopover.hide();
                });

                if ($root.IgniteDemoMode && sessionStorage.showDemoInfo !== 'true') {
                    sessionStorage.showDemoInfo = 'true';

                    DemoInfo.show();
                }
            })
            .catch(Messages.showError)
            .then(() => {
                $scope.ui.ready = true;
                $scope.ui.inputForm && $scope.ui.inputForm.$setPristine();

                Loading.finish('loadingClustersScreen');
            });

        $scope.selectItem = function(item, backup) {
            function selectItem() {
                $scope.selectedItem = item;

                try {
                    if (item && item._id)
                        sessionStorage.lastSelectedCluster = angular.toJson(item._id);
                    else
                        sessionStorage.removeItem('lastSelectedCluster');
                }
                catch (ignored) {
                    // No-op.
                }

                if (backup)
                    $scope.backupItem = backup;
                else if (item)
                    $scope.backupItem = angular.copy(item);
                else
                    $scope.backupItem = emptyCluster;

                $scope.backupItem = angular.merge({}, blank, $scope.backupItem);

                if ($scope.ui.inputForm) {
                    $scope.ui.inputForm.$error = {};
                    $scope.ui.inputForm.$setPristine();
                }

                __original_value = ModelNormalizer.normalize($scope.backupItem);

                if (LegacyUtils.getQueryVariable('new'))
                    $state.go('base.configuration.clusters');
            }

            FormUtils.confirmUnsavedChanges($scope.backupItem && $scope.ui.inputForm && $scope.ui.inputForm.$dirty, selectItem);
        };

        $scope.linkId = () => $scope.backupItem._id ? $scope.backupItem._id : 'create';

        function prepareNewItem(linkId) {
            return angular.merge({}, blank, {
                space: $scope.spaces[0]._id,
                discovery: {
                    kind: 'Multicast',
                    Vm: {addresses: ['127.0.0.1:47500..47510']},
                    Multicast: {addresses: ['127.0.0.1:47500..47510']},
                    Jdbc: {initSchema: true}
                },
                binaryConfiguration: {typeConfigurations: [], compactFooter: true},
                communication: {tcpNoDelay: true},
                connector: {noDelay: true},
                collision: {kind: 'Noop', JobStealing: {stealingEnabled: true}, PriorityQueue: {starvationPreventionEnabled: true}},
                failoverSpi: [],
                logger: {Log4j: { mode: 'Default'}},
                caches: linkId && _.find($scope.caches, {value: linkId}) ? [linkId] : [],
                igfss: linkId && _.find($scope.igfss, {value: linkId}) ? [linkId] : []
            });
        }

        // Add new cluster.
        $scope.createItem = function(linkId) {
            $timeout(() => FormUtils.ensureActivePanel($scope.ui, 'general', 'clusterNameInput'));

            $scope.selectItem(null, prepareNewItem(linkId));
        };

        $scope.indexOfCache = function(cacheId) {
            return _.findIndex($scope.caches, (cache) => cache.value === cacheId);
        };

        function clusterCaches(item) {
            return _.filter(_.map($scope.caches, (scopeCache) => scopeCache.cache),
                (cache) => _.includes(item.caches, cache._id));
        }

        function checkCacheDatasources(item) {
            const caches = clusterCaches(item);

            const checkRes = LegacyUtils.checkDataSources(item, caches);

            if (!checkRes.checked) {
                if (_.get(checkRes.secondObj, 'discovery.kind') === 'Jdbc') {
                    return ErrorPopover.show('dialectInput',
                        'Found cache "' + checkRes.firstObj.name + '" with the same data source bean name "' +
                        item.discovery.Jdbc.dataSourceBean + '" and different database: "' +
                        LegacyUtils.cacheStoreJdbcDialectsLabel(checkRes.secondDB) + '" in current cluster and "' +
                        LegacyUtils.cacheStoreJdbcDialectsLabel(checkRes.firstDB) + '" in "' + checkRes.firstObj.name + '" cache',
                        $scope.ui, 'general', 10000);
                }

                return ErrorPopover.show('cachesInput',
                    'Found caches "' + checkRes.firstObj.name + '" and "' + checkRes.secondObj.name + '" ' +
                    'with the same data source bean name "' + checkRes.firstObj.cacheStoreFactory[checkRes.firstObj.cacheStoreFactory.kind].dataSourceBean +
                    '" and different databases: "' + LegacyUtils.cacheStoreJdbcDialectsLabel(checkRes.firstDB) + '" in "' + checkRes.firstObj.name + '" and "' +
                    LegacyUtils.cacheStoreJdbcDialectsLabel(checkRes.secondDB) + '" in "' + checkRes.secondObj.name + '" cache',
                    $scope.ui, 'general', 10000);
            }

            return true;
        }

        function checkCacheSQLSchemas(item) {
            const caches = clusterCaches(item);

            const checkRes = LegacyUtils.checkCacheSQLSchemas(caches);

            if (!checkRes.checked) {
                return ErrorPopover.show('cachesInput',
                    'Found caches "' + checkRes.firstCache.name + '" and "' + checkRes.secondCache.name + '" ' +
                    'with the same SQL schema name "' + checkRes.firstCache.sqlSchema + '"',
                    $scope.ui, 'general', 10000);
            }

            return true;
        }

        function checkBinaryConfiguration(item) {
            const b = item.binaryConfiguration;

            if (LegacyUtils.isDefined(b)) {
                if (!_.isEmpty(b.typeConfigurations)) {
                    for (let typeIx = 0; typeIx < b.typeConfigurations.length; typeIx++) {
                        const type = b.typeConfigurations[typeIx];

                        if (LegacyUtils.isEmptyString(type.typeName))
                            return ErrorPopover.show('typeName' + typeIx + 'Input', 'Type name should be specified!', $scope.ui, 'binary');

                        if (_.find(b.typeConfigurations, (t, ix) => ix < typeIx && t.typeName === type.typeName))
                            return ErrorPopover.show('typeName' + typeIx + 'Input', 'Type with such name is already specified!', $scope.ui, 'binary');
                    }
                }
            }

            return true;
        }

        function checkCacheKeyConfiguration(item) {
            const cfgs = item.cacheKeyConfiguration;

            if (_.isEmpty(cfgs))
                return true;

            for (let typeIx = 0; typeIx < cfgs.length; typeIx++) {
                const type = cfgs[typeIx];

                if (LegacyUtils.isEmptyString(type.typeName))
                    return ErrorPopover.show('cacheKeyTypeName' + typeIx + 'Input', 'Cache type configuration name should be specified!', $scope.ui, 'cacheKeyCfg');

                if (_.find(cfgs, (t, ix) => ix < typeIx && t.typeName === type.typeName))
                    return ErrorPopover.show('cacheKeyTypeName' + typeIx + 'Input', 'Cache type configuration with such name is already specified!', $scope.ui, 'cacheKeyCfg');
            }

            return true;
        }

        function checkCommunicationConfiguration(item) {
            const c = item.communication;

            if (LegacyUtils.isDefined(c)) {
                if (LegacyUtils.isDefined(c.unacknowledgedMessagesBufferSize)) {
                    if (LegacyUtils.isDefined(c.messageQueueLimit) && c.unacknowledgedMessagesBufferSize < 5 * c.messageQueueLimit)
                        return ErrorPopover.show('unacknowledgedMessagesBufferSizeInput', 'Maximum number of stored unacknowledged messages should be at least 5 * message queue limit!', $scope.ui, 'communication');

                    if (LegacyUtils.isDefined(c.ackSendThreshold) && c.unacknowledgedMessagesBufferSize < 5 * c.ackSendThreshold)
                        return ErrorPopover.show('unacknowledgedMessagesBufferSizeInput', 'Maximum number of stored unacknowledged messages should be at least 5 * ack send threshold!', $scope.ui, 'communication');
                }

                if (c.sharedMemoryPort === 0)
                    return ErrorPopover.show('sharedMemoryPortInput', 'Shared memory port should be more than "0" or equals to "-1"!', $scope.ui, 'communication');
            }

            return true;
        }

        function checkDiscoveryConfiguration(item) {
            const d = item.discovery;

            if (d) {
                if ((_.isNil(d.maxAckTimeout) ? 600000 : d.maxAckTimeout) < (d.ackTimeout || 5000))
                    return ErrorPopover.show('ackTimeoutInput', 'Acknowledgement timeout should be less than max acknowledgement timeout!', $scope.ui, 'discovery');

                if (d.kind === 'Vm' && d.Vm && d.Vm.addresses.length === 0)
                    return ErrorPopover.show('addresses', 'Addresses are not specified!', $scope.ui, 'general');
            }

            return true;
        }

        function checkSwapConfiguration(item) {
            const swapKind = item.swapSpaceSpi && item.swapSpaceSpi.kind;

            if (swapKind && item.swapSpaceSpi[swapKind]) {
                const swap = item.swapSpaceSpi[swapKind];

                const sparsity = swap.maximumSparsity;

                if (LegacyUtils.isDefined(sparsity) && (sparsity < 0 || sparsity >= 1))
                    return ErrorPopover.show('maximumSparsityInput', 'Maximum sparsity should be more or equal 0 and less than 1!', $scope.ui, 'swap');

                const readStripesNumber = swap.readStripesNumber;

                if (readStripesNumber && !(readStripesNumber === -1 || (readStripesNumber & (readStripesNumber - 1)) === 0))
                    return ErrorPopover.show('readStripesNumberInput', 'Read stripe size must be positive and power of two!', $scope.ui, 'swap');
            }

            return true;
        }

        function checkSslConfiguration(item) {
            const r = item.connector;

            if (LegacyUtils.isDefined(r)) {
                if (r.sslEnabled && LegacyUtils.isEmptyString(r.sslFactory))
                    return ErrorPopover.show('connectorSslFactoryInput', 'SSL factory should not be empty!', $scope.ui, 'connector');
            }

            if (item.sslEnabled) {
                if (!LegacyUtils.isDefined(item.sslContextFactory) || LegacyUtils.isEmptyString(item.sslContextFactory.keyStoreFilePath))
                    return ErrorPopover.show('keyStoreFilePathInput', 'Key store file should not be empty!', $scope.ui, 'sslConfiguration');

                if (LegacyUtils.isEmptyString(item.sslContextFactory.trustStoreFilePath) && _.isEmpty(item.sslContextFactory.trustManagers))
                    return ErrorPopover.show('sslConfiguration-title', 'Trust storage file or managers should be configured!', $scope.ui, 'sslConfiguration');
            }

            return true;
        }

        function checkPoolSizes(item) {
            if (item.rebalanceThreadPoolSize && item.systemThreadPoolSize && item.systemThreadPoolSize <= item.rebalanceThreadPoolSize)
                return ErrorPopover.show('rebalanceThreadPoolSizeInput', 'Rebalance thread pool size exceed or equals System thread pool size!', $scope.ui, 'pools');

            return true;
        }

        // Check cluster logical consistency.
        function validate(item) {
            ErrorPopover.hide();

            if (LegacyUtils.isEmptyString(item.name))
                return ErrorPopover.show('clusterNameInput', 'Cluster name should not be empty!', $scope.ui, 'general');

            if (!LegacyUtils.checkFieldValidators($scope.ui))
                return false;

            if (!checkCacheSQLSchemas(item))
                return false;

            if (!checkCacheDatasources(item))
                return false;

            if (!checkBinaryConfiguration(item))
                return false;

            if (!checkCacheKeyConfiguration(item))
                return false;

            if (!checkCommunicationConfiguration(item))
                return false;

            if (!checkDiscoveryConfiguration(item))
                return false;

            if (!checkSwapConfiguration(item))
                return false;

            if (!checkSslConfiguration(item))
                return false;

            if (!checkPoolSizes(item))
                return false;

            return true;
        }

        // Save cluster in database.
        function save(item) {
            $http.post('/api/v1/configuration/clusters/save', item)
                .success(function(_id) {
                    item.label = _clusterLbl(item);

                    $scope.ui.inputForm.$setPristine();

                    const idx = _.findIndex($scope.clusters, (cluster) => cluster._id === _id);

                    if (idx >= 0)
                        angular.merge($scope.clusters[idx], item);
                    else {
                        item._id = _id;
                        $scope.clusters.push(item);
                    }

                    _.forEach($scope.caches, (cache) => {
                        if (_.includes(item.caches, cache.value))
                            cache.cache.clusters = _.union(cache.cache.clusters, [_id]);
                        else
                            _.remove(cache.cache.clusters, (id) => id === _id);
                    });

                    _.forEach($scope.igfss, (igfs) => {
                        if (_.includes(item.igfss, igfs.value))
                            igfs.igfs.clusters = _.union(igfs.igfs.clusters, [_id]);
                        else
                            _.remove(igfs.igfs.clusters, (id) => id === _id);
                    });

                    $scope.selectItem(item);

                    Messages.showInfo('Cluster "' + item.name + '" saved.');
                })
                .error(Messages.showError);
        }

        // Save cluster.
        $scope.saveItem = function() {
            const item = $scope.backupItem;

            const swapSpi = LegacyUtils.autoClusterSwapSpiConfiguration(item, clusterCaches(item));

            if (swapSpi)
                angular.extend(item, swapSpi);

            if (validate(item))
                save(item);
        };

        function _clusterNames() {
            return _.map($scope.clusters, (cluster) => cluster.name);
        }

        // Clone cluster with new name.
        $scope.cloneItem = function() {
            if (validate($scope.backupItem)) {
                Clone.confirm($scope.backupItem.name, _clusterNames()).then(function(newName) {
                    const item = angular.copy($scope.backupItem);

                    delete item._id;
                    item.name = newName;

                    save(item);
                });
            }
        };

        // Remove cluster from db.
        $scope.removeItem = function() {
            const selectedItem = $scope.selectedItem;

            Confirm.confirm('Are you sure you want to remove cluster: "' + selectedItem.name + '"?')
                .then(function() {
                    const _id = selectedItem._id;

                    $http.post('/api/v1/configuration/clusters/remove', {_id})
                        .success(function() {
                            Messages.showInfo('Cluster has been removed: ' + selectedItem.name);

                            const clusters = $scope.clusters;

                            const idx = _.findIndex(clusters, (cluster) => cluster._id === _id);

                            if (idx >= 0) {
                                clusters.splice(idx, 1);

                                $scope.ui.inputForm.$setPristine();

                                if (clusters.length > 0)
                                    $scope.selectItem(clusters[0]);
                                else
                                    $scope.backupItem = emptyCluster;

                                _.forEach($scope.caches, (cache) => _.remove(cache.cache.clusters, (id) => id === _id));
                                _.forEach($scope.igfss, (igfs) => _.remove(igfs.igfs.clusters, (id) => id === _id));
                            }
                        })
                        .error(Messages.showError);
                });
        };

        // Remove all clusters from db.
        $scope.removeAllItems = function() {
            Confirm.confirm('Are you sure you want to remove all clusters?')
                .then(function() {
                    $http.post('/api/v1/configuration/clusters/remove/all')
                        .success(() => {
                            Messages.showInfo('All clusters have been removed');

                            $scope.clusters = [];

                            _.forEach($scope.caches, (cache) => cache.cache.clusters = []);
                            _.forEach($scope.igfss, (igfs) => igfs.igfs.clusters = []);

                            $scope.backupItem = emptyCluster;
                            $scope.ui.inputForm.$error = {};
                            $scope.ui.inputForm.$setPristine();
                        })
                        .error(Messages.showError);
                });
        };

        $scope.resetAll = function() {
            Confirm.confirm('Are you sure you want to undo all changes for current cluster?')
                .then(function() {
                    $scope.backupItem = $scope.selectedItem ? angular.copy($scope.selectedItem) : prepareNewItem();
                    $scope.ui.inputForm.$error = {};
                    $scope.ui.inputForm.$setPristine();
                });
        };
    }
]];
