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

import _ from 'lodash';
import JSZip from 'jszip';
import saver from 'file-saver';

export default [
    '$rootScope', '$scope', '$http', 'IgniteLegacyUtils', 'IgniteMessages', 'IgniteLoading', '$filter', 'igniteConfigurationResource', 'JavaTypes', 'IgniteVersion', 'GeneratorDocker', 'GeneratorPom', 'IgniteFormUtils',
    function($root, $scope, $http, LegacyUtils, Messages, Loading, $filter, Resource, JavaTypes, IgniteVersion, docker, pom, FormUtils) {
        const ctrl = this;

        $scope.ui = { ready: false };

        Loading.start('summaryPage');

        Resource.read()
            .then(Resource.populate)
            .then(({clusters}) => {
                $scope.clusters = clusters;
                $scope.clustersMap = {};
                $scope.clustersView = _.map(clusters, (item) => {
                    const {_id, name} = item;

                    $scope.clustersMap[_id] = item;

                    return {_id, name};
                });

                Loading.finish('summaryPage');

                $scope.ui.ready = true;

                if (!_.isEmpty(clusters)) {
                    const idx = sessionStorage.summarySelectedId || 0;

                    $scope.selectItem(clusters[idx]);
                }
            })
            .catch(Messages.showError);

        $scope.contentVisible = (rows, row) => {
            return !row || !row._id || _.findIndex(rows, (item) => item._id === row._id) >= 0;
        };

        $scope.widthIsSufficient = FormUtils.widthIsSufficient;
        $scope.dialects = {};

        $scope.projectStructureOptions = {
            nodeChildren: 'children',
            dirSelectable: false,
            injectClasses: {
                iExpanded: 'fa fa-folder-open-o',
                iCollapsed: 'fa fa-folder-o'
            },
            equality: (node1, node2) => {
                return node1 === node2;
            }
        };

        const javaConfigFolder = {
            type: 'folder',
            name: 'config',
            children: [
                { type: 'file', name: 'ClientConfigurationFactory.java' },
                { type: 'file', name: 'ServerConfigurationFactory.java' }
            ]
        };

        const javaStartupFolder = {
            type: 'folder',
            name: 'startup',
            children: [
                { type: 'file', name: 'ClientNodeCodeStartup.java' },
                { type: 'file', name: 'ClientNodeSpringStartup.java' },
                { type: 'file', name: 'ServerNodeCodeStartup.java' },
                { type: 'file', name: 'ServerNodeSpringStartup.java' }
            ]
        };

        const demoFolder = {
            type: 'folder',
            name: 'demo',
            children: [
                { type: 'file', name: 'DemoStartup.java' }
            ]
        };

        const resourcesFolder = {
            type: 'folder',
            name: 'resources',
            children: [
                { type: 'file', name: 'secret.properties' }
            ]
        };

        const javaFolder = {
            type: 'folder',
            name: 'java',
            children: [
                {
                    type: 'folder',
                    name: 'config',
                    children: [
                        javaConfigFolder,
                        javaStartupFolder
                    ]
                }
            ]
        };

        const clnCfg = { type: 'file', name: 'client.xml' };

        const srvCfg = { type: 'file', name: 'server.xml' };

        const mainFolder = {
            type: 'folder',
            name: 'main',
            children: [javaFolder]
        };

        const projectStructureRoot = {
            type: 'folder',
            name: 'project.zip',
            children: [
                {
                    type: 'folder',
                    name: 'config',
                    children: [clnCfg, srvCfg]
                },
                {
                    type: 'folder',
                    name: 'jdbc-drivers',
                    children: [
                        { type: 'file', name: 'README.txt' }
                    ]
                },
                {
                    type: 'folder',
                    name: 'src',
                    children: [mainFolder]
                },
                { type: 'file', name: '.dockerignore' },
                { type: 'file', name: 'Dockerfile' },
                { type: 'file', name: 'pom.xml' },
                { type: 'file', name: 'README.txt' }
            ]
        };

        $scope.projectStructure = [projectStructureRoot];

        $scope.projectStructureExpanded = [projectStructureRoot];

        $scope.tabsServer = { activeTab: 0 };
        $scope.tabsClient = { activeTab: 0 };

        /**
         *
         * @param {Object} node - Tree node.
         * @param {string[]} path - Path to find.
         * @returns {Object} Tree node.
         */
        function getOrCreateFolder(node, path) {
            if (_.isEmpty(path))
                return node;

            const leaf = path.shift();

            let children = null;

            if (!_.isEmpty(node.children)) {
                children = _.find(node.children, {type: 'folder', name: leaf});

                if (children)
                    return getOrCreateFolder(children, path);
            }

            children = {type: 'folder', name: leaf, children: []};

            node.children.push(children);

            node.children = _.orderBy(node.children, ['type', 'name'], ['desc', 'asc']);

            return getOrCreateFolder(children, path);
        }

        function addClass(fullClsName) {
            const path = fullClsName.split('.');
            const leaf = {type: 'file', name: path.pop() + '.java'};
            const folder = getOrCreateFolder(javaFolder, path);

            if (!_.find(folder.children, leaf))
                folder.children.push(leaf);
        }

        $scope.selectItem = (cluster) => {
            delete ctrl.cluster;

            if (!cluster)
                return;

            cluster = $scope.clustersMap[cluster._id];

            ctrl.cluster = cluster;

            $scope.cluster = cluster;
            $scope.selectedItem = cluster;
            $scope.dialects = {};

            sessionStorage.summarySelectedId = $scope.clusters.indexOf(cluster);

            mainFolder.children = [javaFolder];
            javaFolder.children = [javaConfigFolder, javaStartupFolder];

            if ($generatorCommon.secretPropertiesNeeded(cluster))
                mainFolder.children.push(resourcesFolder);

            if ($generatorJava.isDemoConfigured(cluster, $root.IgniteDemoMode))
                javaFolder.children.push(demoFolder);

            if (cluster.discovery.kind === 'Jdbc' && cluster.discovery.Jdbc.dialect)
                $scope.dialects[cluster.discovery.Jdbc.dialect] = true;

            _.forEach(cluster.caches, (cache) => {
                if (cache.cacheStoreFactory) {
                    const store = cache.cacheStoreFactory[cache.cacheStoreFactory.kind];

                    if (store && store.dialect)
                        $scope.dialects[store.dialect] = true;
                }

                _.forEach(cache.domains, (domain) => {
                    if (!_.isEmpty(domain.keyFields)) {
                        if (JavaTypes.nonBuiltInClass(domain.keyType))
                            addClass(domain.keyType);

                        addClass(domain.valueType);
                    }
                });
            });

            projectStructureRoot.name = cluster.name + '-project.zip';
            clnCfg.name = cluster.name + '-client.xml';
            srvCfg.name = cluster.name + '-server.xml';
        };

        $scope.$watch('cluster', (cluster) => {
            if (!cluster)
                return;

            if (!$filter('hasPojo')(cluster) && $scope.tabsClient.activeTab === 3)
                $scope.tabsClient.activeTab = 0;
        });

        $scope.$watch('cluster._id', () => {
            $scope.tabsClient.init = [];
            $scope.tabsServer.init = [];
        });

        // TODO IGNITE-2114: implemented as independent logic for download.
        $scope.downloadConfiguration = function() {
            const cluster = $scope.cluster;
            const clientNearCfg = cluster.clientNearCfg;

            const zip = new JSZip();

            if (!ctrl.data)
                ctrl.data = {};

            if (!ctrl.data.docker)
                ctrl.data.docker = docker.generate(cluster, 'latest');

            zip.file('Dockerfile', ctrl.data.docker);
            zip.file('.dockerignore', docker.ignoreFile());

            const builder = $generatorProperties.generateProperties(cluster);

            if (builder)
                zip.file('src/main/resources/secret.properties', builder.asString());

            const srcPath = 'src/main/java/';

            const serverXml = 'config/' + cluster.name + '-server.xml';
            const clientXml = 'config/' + cluster.name + '-client.xml';

            zip.file(serverXml, $generatorXml.cluster(cluster));
            zip.file(clientXml, $generatorXml.cluster(cluster, clientNearCfg));

            zip.file(srcPath + 'config/ServerConfigurationFactory.java', $generatorJava.cluster(cluster, 'config', 'ServerConfigurationFactory', null));
            zip.file(srcPath + 'config/ClientConfigurationFactory.java', $generatorJava.cluster(cluster, 'config', 'ClientConfigurationFactory', clientNearCfg));

            if ($generatorJava.isDemoConfigured(cluster, $root.IgniteDemoMode)) {
                zip.file(srcPath + 'demo/DemoStartup.java', $generatorJava.nodeStartup(cluster, 'demo', 'DemoStartup',
                    'ServerConfigurationFactory.createConfiguration()', 'config.ServerConfigurationFactory'));
            }

            zip.file(srcPath + 'startup/ServerNodeSpringStartup.java', $generatorJava.nodeStartup(cluster, 'startup', 'ServerNodeSpringStartup', '"' + serverXml + '"'));
            zip.file(srcPath + 'startup/ClientNodeSpringStartup.java', $generatorJava.nodeStartup(cluster, 'startup', 'ClientNodeSpringStartup', '"' + clientXml + '"'));

            zip.file(srcPath + 'startup/ServerNodeCodeStartup.java', $generatorJava.nodeStartup(cluster, 'startup', 'ServerNodeCodeStartup',
                'ServerConfigurationFactory.createConfiguration()', 'config.ServerConfigurationFactory'));
            zip.file(srcPath + 'startup/ClientNodeCodeStartup.java', $generatorJava.nodeStartup(cluster, 'startup', 'ClientNodeCodeStartup',
                'ClientConfigurationFactory.createConfiguration()', 'config.ClientConfigurationFactory', clientNearCfg));

            zip.file('pom.xml', pom.generate(cluster, IgniteVersion.version).asString());

            zip.file('README.txt', $generatorReadme.readme().asString());
            zip.file('jdbc-drivers/README.txt', $generatorReadme.readmeJdbc().asString());

            if (!ctrl.data.pojos)
                ctrl.data.pojos = $generatorJava.pojos(cluster.caches);

            for (const pojo of ctrl.data.pojos) {
                if (pojo.keyClass && JavaTypes.nonBuiltInClass(pojo.keyType))
                    zip.file(srcPath + pojo.keyType.replace(/\./g, '/') + '.java', pojo.keyClass);

                zip.file(srcPath + pojo.valueType.replace(/\./g, '/') + '.java', pojo.valueClass);
            }

            $generatorOptional.optionalContent(zip, cluster);

            zip.generateAsync({type: 'blob', compression: 'DEFLATE', mimeType: 'application/octet-stream'})
                .then((blob) => saver.saveAs(blob, cluster.name + '-project.zip'));
        };

        /**
         * @returns {boolean} 'true' if at least one proprietary JDBC driver is configured for cache store.
         */
        $scope.downloadJdbcDriversVisible = function() {
            const dialects = $scope.dialects;

            return !!(dialects.Oracle || dialects.DB2 || dialects.SQLServer);
        };

        /**
         * Open download proprietary JDBC driver pages.
         */
        $scope.downloadJdbcDrivers = function() {
            const dialects = $scope.dialects;

            if (dialects.Oracle)
                window.open('http://www.oracle.com/technetwork/apps-tech/jdbc-112010-090769.html');

            if (dialects.DB2)
                window.open('http://www-01.ibm.com/support/docview.wss?uid=swg21363866');

            if (dialects.SQLServer)
                window.open('https://www.microsoft.com/en-us/download/details.aspx?id=11774');
        };
    }
];
