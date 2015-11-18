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

// Controller for Summary screen.
consoleModule.controller('summaryController', [
    '$scope', '$http', '$common', '$loading', '$message', '$table',
    function ($scope, $http, $common, $loading, $message, $table) {
    var igniteVersion = '1.5.0-IWC';

    $scope.panelExpanded = $common.panelExpanded;
    $scope.tableVisibleRow = $table.tableVisibleRow;
    $scope.joinTip = $common.joinTip;
    $scope.getModel = $common.getModel;
    $scope.widthIsSufficient = $common.widthIsSufficient;

    $scope.showMoreInfo = $message.message;

    $scope.javaClassItems = [
        {label: 'snippet', value: 1},
        {label: 'factory class', value: 2}
    ];

    $scope.evictionPolicies = [
        {value: 'LRU', label: 'LRU'},
        {value: 'FIFO', label: 'FIFO'},
        {value: 'SORTED', label: 'Sorted'},
        {value: undefined, label: 'Not set'}
    ];

    $scope.tabsServer = { activeTab: 0 };
    $scope.tabsClient = { activeTab: 0 };

    $scope.pojoClasses = function() {
        var classes = [];

        if ($scope.selectedItem)
            _.forEach($scope.selectedItem.metadatas, function(meta) {
                classes.push(meta.keyType);
                classes.push(meta.valueType);
            });

        return classes;
    };

    $scope.oss = ['debian:8', 'ubuntu:14.10'];

    $scope.configServer = {javaClassServer: 1, os: undefined};
    $scope.configClient = {};

    $scope.backupItem = {javaClassClient: 1};

    $http.get('/models/summary.json')
        .success(function (data) {
            $scope.screenTip = data.screenTip;
            $scope.moreInfo = data.moreInfo;
            $scope.clientFields = data.clientFields;
        })
        .error(function (errMsg) {
            $common.showError(errMsg);
        });

    $scope.clusters = [];

    $scope.aceInit = function (editor) {
        editor.setReadOnly(true);
        editor.setOption('highlightActiveLine', false);
        editor.setAutoScrollEditorIntoView(true);
        editor.$blockScrolling = Infinity;

        var renderer = editor.renderer;

        renderer.setHighlightGutterLine(false);
        renderer.setShowPrintMargin(false);
        renderer.setOption('fontFamily', 'monospace');
        renderer.setOption('fontSize', '12px');
        renderer.setOption('minLines', '25');
        renderer.setOption('maxLines', '25');

        editor.setTheme('ace/theme/chrome');
    };

    $scope.generateJavaServer = function () {
        $scope.javaServer = $generatorJava.cluster($scope.selectedItem,
            $scope.configServer.javaClassServer === 2 ? 'ServerConfigurationFactory' : false, null, false);
    };

    function selectPojoClass(config) {
        if ($scope.selectedItem)
            _.forEach($scope.selectedItem.metadatas, function(meta) {
                if (meta.keyType == config.pojoClass)
                    return config.pojoClassBody = meta.keyClass;

                if (meta.valueType == config.pojoClass)
                    return config.pojoClassBody = meta.valueClass;
            });
    }

    function pojoClsListener(config) {
        return function () {
            selectPojoClass(config);
        };
    }

    $scope.updatePojos = function() {
        if ($common.isDefined($scope.selectedItem)) {
            var metadatas = $generatorJava.pojos($scope.selectedItem.caches, $scope.configServer.useConstructor, $scope.configServer.includeKeyFields);

            $scope.selectedItem.metadatas = metadatas;

            function restoreSelected(selected, config, tabs, metadatas) {
                if (!$common.isDefined(selected) || _.findIndex(metadatas, function (meta) {
                        return meta.keyType == selected || meta.valueType == selected;
                    }) < 0) {
                    if (metadatas.length > 0) {
                        if ($common.isDefined(metadatas[0].keyType))
                            config.pojoClass = metadatas[0].keyType;
                        else
                            config.pojoClass = metadatas[0].valueType;
                    }
                    else {
                        config.pojoClass = undefined;

                        if (tabs.activeTab == 2)
                            tabs.activeTab = 0;
                    }
                }
                else
                    config.pojoClass = selected;

                selectPojoClass(config);
            }

            restoreSelected($scope.configServer.pojoClass, $scope.configServer, $scope.tabsServer, metadatas);
            restoreSelected($scope.configClient.pojoClass, $scope.configClient, $scope.tabsClient, metadatas);
        }
    };

    $scope.$watch('configServer.javaClassServer', $scope.generateJavaServer, true);

    $scope.$watch('configServer.pojoClass', pojoClsListener($scope.configServer), true);
    $scope.$watch('configClient.pojoClass', pojoClsListener($scope.configClient), true);

    $scope.$watch('configServer.useConstructor', $scope.updatePojos, true);

    $scope.$watch('configServer.includeKeyFields', $scope.updatePojos, true);

    $scope.generateDockerServer = function() {
        var os = $scope.configServer.os ? $scope.configServer.os : $scope.oss[0];

        $scope.dockerServer = $generatorDocker.clusterDocker($scope.selectedItem, os);
    };

    $scope.$watch('configServer.os', $scope.generateDockerServer, true);

    $scope.generateClient = function () {
        $scope.xmlClient = $generatorXml.cluster($scope.selectedItem, $scope.backupItem.nearConfiguration);
        $scope.javaClient = $generatorJava.cluster($scope.selectedItem,
            $scope.backupItem.javaClassClient === 2 ? 'ClientConfigurationFactory' : false,
            $scope.backupItem.nearConfiguration, true);
    };

    $scope.$watch('backupItem', $scope.generateClient, true);

    $scope.selectItem = function (cluster) {
        if (!cluster)
            return;

        $scope.selectedItem = cluster;

        $scope.xmlServer = $generatorXml.cluster(cluster);

        $scope.pom = $generatorPom.pom(cluster, igniteVersion).asString();

        $scope.generateJavaServer();

        $scope.generateDockerServer();

        $scope.generateClient();

        $scope.updatePojos();
    };

    $scope.pojoAvailable = function() {
        return $scope.selectedItem && $common.isDefined($scope.selectedItem.metadatas) && $scope.selectedItem.metadatas.length > 0;
    };

    $scope.downloadConfiguration = function () {
        var cluster = $scope.selectedItem;
        var clientNearConfiguration = $scope.backupItem.nearConfiguration;

        var zip = new JSZip();

        zip.file('Dockerfile', $scope.dockerServer);

        var builder = $generatorProperties.sslProperties(cluster);

        builder = $generatorProperties.dataSourcesProperties(cluster, builder);

        if (builder)
            zip.file('src/main/resources/secret.properties', builder.asString());

        var srcPath = 'src/main/java/';

        zip.file('config/' + cluster.name + '-server.xml', $generatorXml.cluster(cluster));
        zip.file('config/' + cluster.name + '-client.xml', $generatorXml.cluster(cluster, clientNearConfiguration));

        zip.file(srcPath + 'ServerConfigurationFactory.java', $generatorJava.cluster(cluster, 'ServerConfigurationFactory', null, false));
        zip.file(srcPath + 'ClientConfigurationFactory.java', $generatorJava.cluster(cluster, 'ClientConfigurationFactory', clientNearConfiguration, true));
        zip.file(srcPath + 'NodeStartup.java', $generatorJava.nodeStartup(cluster));

        zip.file('pom.xml', $generatorPom.pom(cluster, igniteVersion).asString());

        zip.file('README.txt', $generatorReadme.readme().asString());
        zip.file('jdbc-drivers/README.txt', $generatorReadme.readmeJdbc().asString());

        for (var meta of $generatorJava.pojos(cluster.caches, $scope.configServer.useConstructor, $scope.configServer.includeKeyFields)) {
            if (meta.keyClass)
                zip.file(srcPath + meta.keyType.replace(/\./g, '/') + '.java', meta.keyClass);

            zip.file(srcPath + meta.valueType.replace(/\./g, '/') + '.java', meta.valueClass);
        }

        var blob = zip.generate({type:'blob', mimeType: 'application/octet-stream'});

        // Download archive.
        saveAs(blob, cluster.name + '-configuration.zip');
    };

    $loading.start('loadingSummaryScreen');

    $http.post('clusters/list')
        .success(function (data) {
            $scope.clusters = data.clusters;

            if ($scope.clusters.length > 0) {
                // Populate clusters with caches.
                _.forEach($scope.clusters, function (cluster) {
                    cluster.caches = _.filter(data.caches, function (cache) {
                        return _.contains(cluster.caches, cache._id);
                    });

                    cluster.igfss = _.filter(data.igfss, function (igfs) {
                        return _.contains(cluster.igfss, igfs._id);
                    });
                });

                var restoredId = sessionStorage.summarySelectedId;

                var selectIdx = 0;

                if (restoredId) {
                    var idx = _.findIndex($scope.clusters, function (cluster) {
                        return cluster._id == restoredId;
                    });

                    if (idx >= 0)
                        selectIdx = idx;
                    else
                        delete sessionStorage.summarySelectedId;
                }

                $scope.selectItem($scope.clusters[selectIdx]);

                $scope.$watch('selectedItem', function (val) {
                    if (val)
                        sessionStorage.summarySelectedId = val._id;
                }, true);
            }
        })
        .finally(function () {
            $loading.finish('loadingSummaryScreen');
        });
}]);
