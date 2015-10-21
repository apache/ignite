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
consoleModule.controller('clustersController', [
    '$scope', '$controller', '$http', '$timeout', '$common', '$focus', '$confirm', '$message', '$clone', '$table', '$preview', '$loading', '$unsavedChangesGuard',
    function ($scope, $controller, $http, $timeout, $common, $focus, $confirm, $message, $clone, $table, $preview, $loading, $unsavedChangesGuard) {
        $unsavedChangesGuard.install($scope);

        // Initialize the super class and extend it.
        angular.extend(this, $controller('save-remove', {$scope: $scope}));

        $scope.ui = $common.formUI();

        $scope.showMoreInfo = $message.message;

        $scope.joinTip = $common.joinTip;
        $scope.getModel = $common.getModel;
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

        var previews = [];

        $scope.previewInit = function (preview) {
            previews.push(preview);

            $preview.previewInit(preview);
        };

        $scope.trustManagersConfigured = function() {
            return $scope.backupItem.sslEnabled && $common.isDefined($scope.backupItem.sslContextFactory)
                && !$common.isEmptyArray($scope.backupItem.sslContextFactory.trustManagers)
        };

        $scope.previewChanged = $preview.previewChanged;

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

        $scope.events = [];

        for (var eventGroupName in $dataStructures.EVENT_GROUPS) {
            if ($dataStructures.EVENT_GROUPS.hasOwnProperty(eventGroupName)) {
                $scope.events.push({value: eventGroupName, label: eventGroupName});
            }
        }

        $scope.preview = {
            general: {xml: '', java: '', allDefaults: true},
            atomics: {xml: '', java: '', allDefaults: true},
            communication: {xml: '', java: '', allDefaults: true},
            deployment: {xml: '', java: '', allDefaults: true},
            discovery: {xml: '', java: '', allDefaults: true},
            events: {xml: '', java: '', allDefaults: true},
            marshaller: {xml: '', java: '', allDefaults: true},
            metrics: {xml: '', java: '', allDefaults: true},
            swap: {xml: '', java: '', allDefaults: true},
            time: {xml: '', java: '', allDefaults: true},
            pools: {xml: '', java: '', allDefaults: true},
            transactions: {xml: '', java: '', allDefaults: true},
            sslConfiguration: {xml: '', java: '', allDefaults: true}
        };

        $scope.cacheModes = $common.mkOptions(['LOCAL', 'REPLICATED', 'PARTITIONED']);

        $scope.deploymentModes = $common.mkOptions(['PRIVATE', 'ISOLATED', 'SHARED', 'CONTINUOUS']);

        $scope.transactionConcurrency = $common.mkOptions(['OPTIMISTIC', 'PESSIMISTIC']);

        $scope.transactionIsolation = $common.mkOptions(['READ_COMMITTED', 'REPEATABLE_READ', 'SERIALIZABLE']);

        $scope.segmentationPolicy = $common.mkOptions(['RESTART_JVM', 'STOP', 'NOOP']);

        $scope.marshallers = $common.mkOptions(['OptimizedMarshaller', 'JdkMarshaller']);

        $scope.sslKeyAlgorithms = ['SumX509', 'X509'];

        $scope.sslStoreType = ['JKS', 'PCKS11', 'PCKS12'];

        $scope.sslProtocols = ['TSL', 'SSL'];

        $scope.toggleExpanded = function () {
            $scope.ui.expanded = !$scope.ui.expanded;

            $common.hidePopover();
        };

        $scope.panels = {activePanels: [0]};

        var simpleTables = {
            addresses: {msg: 'Such IP address already exists!', id: 'IpAddress'},
            regions: {msg: 'Such region already exists!', id: 'Region'},
            zones: {msg: 'Such zone already exists!', id: 'Zone'},
            peerClassLoadingLocalClassPathExclude: {msg: 'Such package already exists!', id: 'PeerClsPathExclude'},
            trustManagers: {msg: 'Such trust manager already exists!', id: 'trustManagers'}
        };

        $scope.tableSimpleValid = function (item, field, val, index) {
            var model = $common.getModel(item, field)[field.model];

            if (field.model == 'trustManagers' && !$common.isValidJavaClass('Trust manager', val, false,  $table.tableFieldId(index, 'trustManagers'), false))
                return false;

            if ($common.isDefined(model)) {
                var idx = _.indexOf(model, val);

                // Found duplicate.
                if (idx >= 0 && idx != index) {
                    var simpleTable = simpleTables[field.model];

                    if (simpleTable) {
                        $common.showPopoverMessage(null, null, $table.tableFieldId(index, 'trustManagers'), simpleTable.msg);

                        return $table.tableFocusInvalidField(index, simpleTable.id);
                    }
                }
            }

            return true;
        };

        $scope.clusters = [];

        function selectFirstItem() {
            if ($scope.clusters.length > 0)
                $scope.selectItem($scope.clusters[0]);
        }

        $loading.start('loadingClustersScreen');

        // When landing on the page, get clusters and show them.
        $http.post('clusters/list')
            .success(function (data) {
                $scope.spaces = data.spaces;
                $scope.clusters = data.clusters;
                $scope.caches = _.map(data.caches, function (cache) {
                    return {value: cache._id, label: cache.name, cache: cache};
                });

                // Load page descriptor.
                $http.get('/models/clusters.json')
                    .success(function (data) {
                        $scope.screenTip = data.screenTip;
                        $scope.moreInfo = data.moreInfo;
                        $scope.general = data.general;
                        $scope.advanced = data.advanced;

                        $scope.ui.addGroups(data.general, data.advanced);

                        if ($common.getQueryVariable('new'))
                            $scope.createItem($common.getQueryVariable('id'));
                        else {
                            var lastSelectedCluster = angular.fromJson(sessionStorage.lastSelectedCluster);

                            if (lastSelectedCluster) {
                                var idx = _.findIndex($scope.clusters, function (cluster) {
                                    return cluster._id == lastSelectedCluster;
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

                        $scope.$watch('backupItem', function (val) {
                            if (val) {
                                var clusterCaches = _.reduce($scope.caches, function(caches, cache){
                                    if (_.contains(val.caches, cache.value)) {
                                        caches.push(cache.cache);
                                    }

                                    return caches;
                                }, []);

                                var srcItem = $scope.selectedItem ? $scope.selectedItem : prepareNewItem();

                                $scope.ui.checkDirty(val, srcItem);

                                $scope.preview.general.xml = $generatorXml.clusterCaches(clusterCaches, $generatorXml.clusterGeneral(val)).asString();
                                $scope.preview.general.java = $generatorJava.clusterCaches(clusterCaches, $generatorJava.clusterGeneral(val)).asString();
                                $scope.preview.general.allDefaults = $common.isEmptyString($scope.preview.general.xml);

                                $scope.preview.atomics.xml = $generatorXml.clusterAtomics(val).asString();
                                $scope.preview.atomics.java = $generatorJava.clusterAtomics(val).asString();
                                $scope.preview.atomics.allDefaults = $common.isEmptyString($scope.preview.atomics.xml);

                                $scope.preview.communication.xml = $generatorXml.clusterCommunication(val).asString();
                                $scope.preview.communication.java = $generatorJava.clusterCommunication(val).asString();
                                $scope.preview.communication.allDefaults = $common.isEmptyString($scope.preview.communication.xml);

                                $scope.preview.deployment.xml = $generatorXml.clusterDeployment(val).asString();
                                $scope.preview.deployment.java = $generatorJava.clusterDeployment(val).asString();
                                $scope.preview.deployment.allDefaults = $common.isEmptyString($scope.preview.deployment.xml);

                                $scope.preview.discovery.xml = $generatorXml.clusterDiscovery(val.discovery).asString();
                                $scope.preview.discovery.java = $generatorJava.clusterDiscovery(val.discovery).asString();
                                $scope.preview.discovery.allDefaults = $common.isEmptyString($scope.preview.discovery.xml);

                                $scope.preview.events.xml = $generatorXml.clusterEvents(val).asString();
                                $scope.preview.events.java = $generatorJava.clusterEvents(val).asString();
                                $scope.preview.events.allDefaults = $common.isEmptyString($scope.preview.events.xml);

                                $scope.preview.marshaller.xml = $generatorXml.clusterMarshaller(val).asString();
                                $scope.preview.marshaller.java = $generatorJava.clusterMarshaller(val).asString();
                                $scope.preview.marshaller.allDefaults = $common.isEmptyString($scope.preview.marshaller.xml);

                                $scope.preview.metrics.xml = $generatorXml.clusterMetrics(val).asString();
                                $scope.preview.metrics.java = $generatorJava.clusterMetrics(val).asString();
                                $scope.preview.metrics.allDefaults = $common.isEmptyString($scope.preview.metrics.xml);

                                $scope.preview.swap.xml = $generatorXml.clusterSwap(val).asString();
                                $scope.preview.swap.java = $generatorJava.clusterSwap(val).asString();
                                $scope.preview.swap.allDefaults = $common.isEmptyString($scope.preview.swap.xml);

                                $scope.preview.time.xml = $generatorXml.clusterTime(val).asString();
                                $scope.preview.time.java = $generatorJava.clusterTime(val).asString();
                                $scope.preview.time.allDefaults = $common.isEmptyString($scope.preview.time.xml);

                                $scope.preview.pools.xml = $generatorXml.clusterPools(val).asString();
                                $scope.preview.pools.java = $generatorJava.clusterPools(val).asString();
                                $scope.preview.pools.allDefaults = $common.isEmptyString($scope.preview.pools.xml);

                                $scope.preview.transactions.xml = $generatorXml.clusterTransactions(val).asString();
                                $scope.preview.transactions.java = $generatorJava.clusterTransactions(val).asString();
                                $scope.preview.transactions.allDefaults = $common.isEmptyString($scope.preview.transactions.xml);

                                $scope.preview.sslConfiguration.xml = $generatorXml.clusterSsl(val).asString();
                                $scope.preview.sslConfiguration.java = $generatorJava.clusterSsl(val).asString();
                                $scope.preview.sslConfiguration.allDefaults = $common.isEmptyString($scope.preview.sslConfiguration.xml);
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
                $loading.finish('loadingClustersScreen');
            });

        $scope.selectItem = function (item, backup) {
            function selectItem() {
                $table.tableReset();

                $scope.selectedItem = angular.copy(item);

                try {
                    if (item && item._id)
                        sessionStorage.lastSelectedCluster = angular.toJson(item._id);
                    else
                        sessionStorage.removeItem('lastSelectedCluster');
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
                'Selected cluster: ' + $scope.backupItem.name : 'New cluster';
        };

        function prepareNewItem(cacheId) {
            var newItem = {
                discovery: {kind: 'Multicast', Vm: {addresses: ['127.0.0.1:47500..47510']}, Multicast: {}},
                deploymentMode: 'SHARED'
            };

            newItem.caches = cacheId && _.find($scope.caches, {value: cacheId}) ? [cacheId] : [];
            newItem.space = $scope.spaces[0]._id;

            return newItem;
        }

        // Add new cluster.
        $scope.createItem = function(cacheId) {
            $table.tableReset();

            $timeout(function () {
                $common.ensureActivePanel($scope.panels, "general", 'clusterName');
            });

            $scope.selectItem(undefined, prepareNewItem(cacheId));
        };

        $scope.indexOfCache = function (cacheId) {
            return _.findIndex($scope.caches, function (cache) {
                return cache.value == cacheId;
            });
        };

        // Check cluster logical consistency.
        function validate(item) {
            if ($common.isEmptyString(item.name))
                return showPopoverMessage($scope.panels, 'general', 'clusterName', 'Name should not be empty');

            var c = item.communication;

            if ($common.isDefined(c)) {
                if (!$common.isEmptyString(c.listener) && !$common.isValidJavaClass('Communication listener', c.listener, false, 'comListener', false, $scope.panels, 'communication'))
                    return false;

                if (!$common.isEmptyString(c.addressResolver) && !$common.isValidJavaClass('Address resolver', c.addressResolver, false, 'comAddressResolver', false, $scope.panels, 'communication'))
                    return false;

                if ($common.isDefined(c.unacknowledgedMessagesBufferSize)) {
                    if ($common.isDefined(c.messageQueueLimit))
                        if (c.unacknowledgedMessagesBufferSize < 5 * c.messageQueueLimit)
                            return showPopoverMessage($scope.panels, 'communication', 'unacknowledgedMessagesBufferSize', 'Maximum number of stored unacknowledged messages should be at least 5 * message queue limit');

                    if ($common.isDefined(c.ackSendThreshold))
                        if (c.unacknowledgedMessagesBufferSize < 5 * c.ackSendThreshold)
                            return showPopoverMessage($scope.panels, 'communication', 'unacknowledgedMessagesBufferSize', 'Maximum number of stored unacknowledged messages should be at least 5 * ack send threshold');
                }
            }

            var d = item.discovery;

            if (!$common.isEmptyString(d.addressResolver) && !$common.isValidJavaClass('Address resolver', d.addressResolver, false, 'discoAddressResolver', false, $scope.panels, 'discovery'))
                return false;

            if (!$common.isEmptyString(d.listener) && !$common.isValidJavaClass('Discovery listener', d.listener, false, 'discoListener', false, $scope.panels, 'discovery'))
                return false;

            if (!$common.isEmptyString(d.dataExchange) && !$common.isValidJavaClass('Data exchange', d.dataExchange, false, 'dataExchange', false, $scope.panels, 'discovery'))
                return false;

            if (!$common.isEmptyString(d.metricsProvider) && !$common.isValidJavaClass('Metrics provider', d.metricsProvider, false, 'metricsProvider', false, $scope.panels, 'discovery'))
                return false;

            if (!$common.isEmptyString(d.authenticator) && !$common.isValidJavaClass('Node authenticator', d.authenticator, false, 'authenticator', false, $scope.panels, 'discovery'))
                return false;

            if (item.discovery.kind == 'Vm' && item.discovery.Vm.addresses.length == 0)
                return showPopoverMessage($scope.panels, 'general', 'addresses', 'Addresses are not specified');

            if (item.discovery.kind == 'S3' && $common.isEmptyString(item.discovery.S3.bucketName))
                return showPopoverMessage($scope.panels, 'general', 'bucketName', 'Bucket name should not be empty');

            if (item.discovery.kind == 'Cloud') {
                if ($common.isEmptyString(item.discovery.Cloud.identity))
                    return showPopoverMessage($scope.panels, 'general', 'identity', 'Identity should not be empty');

                if ($common.isEmptyString(item.discovery.Cloud.provider))
                    return showPopoverMessage($scope.panels, 'general', 'provider', 'Provider should not be empty');
            }

            if (item.discovery.kind == 'GoogleStorage') {
                if ($common.isEmptyString(item.discovery.GoogleStorage.projectName))
                    return showPopoverMessage($scope.panels, 'general', 'projectName', 'Project name should not be empty');

                if ($common.isEmptyString(item.discovery.GoogleStorage.bucketName))
                    return showPopoverMessage($scope.panels, 'general', 'bucketName', 'Bucket name should not be empty');

                if ($common.isEmptyString(item.discovery.GoogleStorage.serviceAccountP12FilePath))
                    return showPopoverMessage($scope.panels, 'general', 'serviceAccountP12FilePath', 'Private key path should not be empty');

                if ($common.isEmptyString(item.discovery.GoogleStorage.serviceAccountId))
                    return showPopoverMessage($scope.panels, 'general', 'serviceAccountId', 'Account ID should not be empty');
            }

            if (item.sslEnabled) {
                if (!$common.isDefined(item.sslContextFactory)
                    || $common.isEmptyString(item.sslContextFactory.keyStoreFilePath))
                    return showPopoverMessage($scope.panels, 'sslConfiguration', 'keyStoreFilePath', 'Key store file should not be empty');

                if ($common.isEmptyString(item.sslContextFactory.trustStoreFilePath) && $common.isEmptyArray(item.sslContextFactory.trustManagers))
                    return showPopoverMessage($scope.panels, 'sslConfiguration', 'sslConfiguration-title', 'Trust storage file or managers should be configured');
            }

            if (!item.swapSpaceSpi || !item.swapSpaceSpi.kind && item.caches) {
                for (var i = 0; i < item.caches.length; i++) {
                    var idx = $scope.indexOfCache(item.caches[i]);

                    if (idx >= 0) {
                        var cache = $scope.caches[idx];

                        if (cache.cache.swapEnabled) {
                            $scope.ui.expanded = true;

                            return showPopoverMessage($scope.panels, 'swap', 'swapSpaceSpi',
                                'Swap space SPI is not configured, but cache "' + cache.label + '" configured to use swap!');
                        }
                    }
                }
            }

            return true;
        }

        // Save cluster in database.
        function save(item) {
            $http.post('clusters/save', item)
                .success(function (_id) {
                    $scope.ui.markPristine();

                    var idx = _.findIndex($scope.clusters, function (cluster) {
                        return cluster._id == _id;
                    });

                    if (idx >= 0)
                        angular.extend($scope.clusters[idx], item);
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
            $table.tableReset();

            var item = $scope.backupItem;

            if (validate(item))
                save(item);
        };

        // Copy cluster with new name.
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

        // Remove cluster from db.
        $scope.removeItem = function () {
            $table.tableReset();

            var selectedItem = $scope.selectedItem;

            $confirm.confirm('Are you sure you want to remove cluster: "' + selectedItem.name + '"?')
                .then(function () {
                        var _id = selectedItem._id;

                        $http.post('clusters/remove', {_id: _id})
                            .success(function () {
                                $common.showInfo('Cluster has been removed: ' + selectedItem.name);

                                var clusters = $scope.clusters;

                                var idx = _.findIndex(clusters, function (cluster) {
                                    return cluster._id == _id;
                                });

                                if (idx >= 0) {
                                    clusters.splice(idx, 1);

                                    if (clusters.length > 0)
                                        $scope.selectItem(clusters[0]);
                                    else
                                        $scope.selectItem(undefined, undefined);
                                }
                            })
                            .error(function (errMsg) {
                                $common.showError(errMsg);
                            });
                });
        };

        // Remove all clusters from db.
        $scope.removeAllItems = function () {
            $table.tableReset();

            $confirm.confirm('Are you sure you want to remove all clusters?')
                .then(function () {
                        $http.post('clusters/remove/all')
                            .success(function () {
                                $common.showInfo('All clusters have been removed');

                                $scope.clusters = [];

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
    }]
);
