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
    '$rootScope', '$scope', '$http', 'IgniteLegacyUtils', 'IgniteMessages', 'IgniteLoading', '$filter', 'IgniteConfigurationResource', 'JavaTypes', 'IgniteVersion', 'IgniteConfigurationGenerator', 'SpringTransformer', 'JavaTransformer', 'GeneratorDocker', 'GeneratorPom', 'IgnitePropertiesGenerator', 'IgniteReadmeGenerator', 'IgniteFormUtils',
    function($root, $scope, $http, LegacyUtils, Messages, Loading, $filter, Resource, JavaTypes, Version, generator, spring, java, docker, pom, propsGenerator, readme, FormUtils) {
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

        const loadFolder = {
            type: 'folder',
            name: 'load',
            children: [
                { type: 'file', name: 'LoadCaches.java' }
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

        const clnCfg = { type: 'file', name: 'client.xml' };
        const srvCfg = { type: 'file', name: 'server.xml' };

        const resourcesFolder = {
            type: 'folder',
            name: 'resources',
            children: [
                {
                    type: 'folder',
                    name: 'META-INF',
                    children: [clnCfg, srvCfg]
                }
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

        function cacheHasDatasource(cache) {
            if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind) {
                const storeFactory = cache.cacheStoreFactory[cache.cacheStoreFactory.kind];

                return !!(storeFactory && (storeFactory.connectVia ? (storeFactory.connectVia === 'DataSource' ? storeFactory.dialect : false) : storeFactory.dialect)); // eslint-disable-line no-nested-ternary
            }

            return false;
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

            mainFolder.children = [javaFolder, resourcesFolder];

            if (_.find(cluster.caches, (cache) => !_.isNil(cache.cacheStoreFactory)))
                javaFolder.children = [javaConfigFolder, loadFolder, javaStartupFolder];
            else
                javaFolder.children = [javaConfigFolder, javaStartupFolder];

            if (_.nonNil(_.find(cluster.caches, cacheHasDatasource)) || cluster.sslEnabled)
                resourcesFolder.children.push({ type: 'file', name: 'secret.properties' });

            if (java.isDemoConfigured(cluster, $root.IgniteDemoMode))
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

            const zip = new JSZip();

            if (!ctrl.data)
                ctrl.data = {};

            if (!ctrl.data.docker)
                ctrl.data.docker = docker.generate(cluster, 'latest');

            zip.file('Dockerfile', ctrl.data.docker);
            zip.file('.dockerignore', docker.ignoreFile());

            const cfg = generator.igniteConfiguration(cluster, false);
            const clientCfg = generator.igniteConfiguration(cluster, true);
            const clientNearCaches = _.filter(cluster.caches, (cache) => _.get(cache, 'clientNearConfiguration.enabled'));

            const secProps = propsGenerator.generate(cfg);

            if (secProps)
                zip.file('src/main/resources/secret.properties', secProps);

            const srcPath = 'src/main/java';
            const resourcesPath = 'src/main/resources';

            const serverXml = `${cluster.name}-server.xml`;
            const clientXml = `${cluster.name}-client.xml`;

            const metaPath = `${resourcesPath}/META-INF`;

            zip.file(`${metaPath}/${serverXml}`, spring.igniteConfiguration(cfg).asString());
            zip.file(`${metaPath}/${clientXml}`, spring.igniteConfiguration(clientCfg, clientNearCaches).asString());

            const cfgPath = `${srcPath}/config`;

            zip.file(`${cfgPath}/ServerConfigurationFactory.java`, java.igniteConfiguration(cfg, 'config', 'ServerConfigurationFactory').asString());
            zip.file(`${cfgPath}/ClientConfigurationFactory.java`, java.igniteConfiguration(cfg, 'config', 'ClientConfigurationFactory', clientNearCaches).asString());

            if (java.isDemoConfigured(cluster, $root.IgniteDemoMode)) {
                zip.file(`${srcPath}/demo/DemoStartup.java`, java.nodeStartup(cluster, 'demo.DemoStartup',
                    'ServerConfigurationFactory.createConfiguration()', 'config.ServerConfigurationFactory'));
            }

            // Generate loader for caches with configured store.
            const cachesToLoad = _.filter(cluster.caches, (cache) => _.nonNil(cache.cacheStoreFactory));

            if (_.nonEmpty(cachesToLoad))
                zip.file(`${srcPath}/load/LoadCaches.java`, java.loadCaches(cachesToLoad, 'load', 'LoadCaches', `"${clientXml}"`));

            const startupPath = `${srcPath}/startup`;

            zip.file(`${startupPath}/ServerNodeSpringStartup.java`, java.nodeStartup(cluster, 'startup.ServerNodeSpringStartup', `"${serverXml}"`));
            zip.file(`${startupPath}/ClientNodeSpringStartup.java`, java.nodeStartup(cluster, 'startup.ClientNodeSpringStartup', `"${clientXml}"`));

            zip.file(`${startupPath}/ServerNodeCodeStartup.java`, java.nodeStartup(cluster, 'startup.ServerNodeCodeStartup',
                'ServerConfigurationFactory.createConfiguration()', 'config.ServerConfigurationFactory'));
            zip.file(`${startupPath}/ClientNodeCodeStartup.java`, java.nodeStartup(cluster, 'startup.ClientNodeCodeStartup',
                'ClientConfigurationFactory.createConfiguration()', 'config.ClientConfigurationFactory', clientNearCaches));

            zip.file('pom.xml', pom.generate(cluster, Version.productVersion().ignite).asString());

            zip.file('README.txt', readme.generate());
            zip.file('jdbc-drivers/README.txt', readme.generateJDBC());

            if (_.isEmpty(ctrl.data.pojos))
                ctrl.data.pojos = java.pojos(cluster.caches);

            for (const pojo of ctrl.data.pojos) {
                if (pojo.keyClass && JavaTypes.nonBuiltInClass(pojo.keyType))
                    zip.file(`${srcPath}/${pojo.keyType.replace(/\./g, '/')}.java`, pojo.keyClass);

                zip.file(`${srcPath}/${pojo.valueType.replace(/\./g, '/')}.java`, pojo.valueClass);
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
