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

export default ['$scope', '$http', '$common', '$loading', '$table', function($scope, $http, $common, $loading, $table) {
    var ctrl = this;

    var igniteVersion = '1.5.0';

    $scope.panelExpanded = $common.panelExpanded;
    $scope.tableVisibleRow = $table.tableVisibleRow;
    $scope.joinTip = $common.joinTip;
    $scope.getModel = $common.getModel;
    $scope.widthIsSufficient = $common.widthIsSufficient;

    $scope.evictionPolicies = [
        {value: 'LRU', label: 'LRU'},
        {value: 'FIFO', label: 'FIFO'},
        {value: 'SORTED', label: 'Sorted'},
        {value: undefined, label: 'Not set'}
    ];

    $scope.tabsServer = { activeTab: 0 };
    $scope.tabsClient = { activeTab: 0 };

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

    $scope.selectItem = function (cluster) {
        if (!cluster)
            return;

        $scope.cluster = cluster;

        $scope.selectedItem = cluster;
    };

    $scope.pojoAvailable = function() {
        let cachesFilter = (cache) => {
            return cache.metadatas && cache.metadatas.length;
        }

        return $scope.cluster && $scope.cluster.caches && _.chain($scope.cluster.caches).filter(cachesFilter).first().value();
    };

    $scope.$watch('cluster', function() {
        if (!$scope.pojoAvailable() &&  $scope.tabsClient.activeTab === 3) {
             $scope.tabsClient.activeTab = 0;
        }
    })

    $scope.downloadConfiguration = function () {
        var cluster = $scope.selectedItem;
        var clientNearCfg = $scope.backupItem.nearConfiguration;

        var zip = new JSZip();

        zip.file('Dockerfile', $scope.dockerServer);

        var builder = $generatorProperties.generateProperties(cluster);

        if (builder)
            zip.file('src/main/resources/secret.properties', builder.asString());

        var srcPath = 'src/main/java/';

        var serverXml = 'config/' + cluster.name + '-server.xml';
        var clientXml = 'config/' + cluster.name + '-client.xml';

        zip.file(serverXml, $generatorXml.cluster(cluster));
        zip.file(clientXml , $generatorXml.cluster(cluster, clientNearCfg));

        zip.file(srcPath + 'factory/ServerConfigurationFactory.java', $generatorJava.cluster(cluster, 'factory', 'ServerConfigurationFactory', null));
        zip.file(srcPath + 'factory/ClientConfigurationFactory.java', $generatorJava.cluster(cluster, 'factory', 'ClientConfigurationFactory', clientNearCfg));

        zip.file(srcPath + 'startup/ServerNodeSpringStartup.java', $generatorJava.nodeStartup(cluster, 'startup', 'ServerNodeSpringStartup', '"' + serverXml + '"'));
        zip.file(srcPath + 'startup/ClientNodeSpringStartup.java', $generatorJava.nodeStartup(cluster, 'startup', 'ClientNodeSpringStartup', '"' + clientXml + '"'));

        zip.file(srcPath + 'startup/ServerNodeCodeStartup.java', $generatorJava.nodeStartup(cluster, 'startup', 'ServerNodeCodeStartup',
            'ServerConfigurationFactory.createConfiguration()', 'factory.ServerConfigurationFactory'));
        zip.file(srcPath + 'startup/ClientNodeCodeStartup.java', $generatorJava.nodeStartup(cluster, 'startup', 'ClientNodeCodeStartup',
            'ClientConfigurationFactory.createConfiguration()', 'factory.ClientConfigurationFactory', clientNearCfg));

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

    $http.post('/api/v1/configuration/clusters/list')
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
                        return cluster._id === restoredId;
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
}]