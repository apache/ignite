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

import JSZip from 'jszip';

import IgniteMavenGenerator from 'app/modules/configuration/generator/Maven.service';
import IgniteDockerGenerator from 'app/modules/configuration/generator/Docker.service';
import IgniteReadmeGenerator from 'app/modules/configuration/generator/Readme.service';
import IgnitePropertiesGenerator from 'app/modules/configuration/generator/Properties.service';
import IgniteConfigurationGenerator from 'app/modules/configuration/generator/ConfigurationGenerator';

import IgniteJavaTransformer from 'app/modules/configuration/generator/JavaTransformer.service';
import IgniteSpringTransformer from 'app/modules/configuration/generator/SpringTransformer.service';

const maven = new IgniteMavenGenerator();
const docker = new IgniteDockerGenerator();
const readme = new IgniteReadmeGenerator();
const properties = new IgnitePropertiesGenerator();

const java = IgniteJavaTransformer;
const spring = IgniteSpringTransformer;

const generator = IgniteConfigurationGenerator;

const escapeFileName = (name) => name.replace(/[\\\/*\"\[\],\.:;|=<>?]/g, '-').replace(/ /g, '_');

// eslint-disable-next-line no-undef
onmessage = function(e) {
    const {cluster, data, demo} = e.data;

    const zip = new JSZip();

    if (!data.docker)
        data.docker = docker.generate(cluster, 'latest');

    zip.file('Dockerfile', data.docker);
    zip.file('.dockerignore', docker.ignoreFile());

    const cfg = generator.igniteConfiguration(cluster, false);
    const clientCfg = generator.igniteConfiguration(cluster, true);
    const clientNearCaches = _.filter(cluster.caches, (cache) => _.get(cache, 'clientNearConfiguration.enabled'));

    const secProps = properties.generate(cfg);

    if (secProps)
        zip.file('src/main/resources/secret.properties', secProps);

    const srcPath = 'src/main/java';
    const resourcesPath = 'src/main/resources';

    const serverXml = `${escapeFileName(cluster.name)}-server.xml`;
    const clientXml = `${escapeFileName(cluster.name)}-client.xml`;

    const metaPath = `${resourcesPath}/META-INF`;

    zip.file(`${metaPath}/${serverXml}`, spring.igniteConfiguration(cfg).asString());
    zip.file(`${metaPath}/${clientXml}`, spring.igniteConfiguration(clientCfg, clientNearCaches).asString());

    const cfgPath = `${srcPath}/config`;

    zip.file(`${cfgPath}/ServerConfigurationFactory.java`, java.igniteConfiguration(cfg, 'config', 'ServerConfigurationFactory').asString());
    zip.file(`${cfgPath}/ClientConfigurationFactory.java`, java.igniteConfiguration(clientCfg, 'config', 'ClientConfigurationFactory', clientNearCaches).asString());

    if (java.isDemoConfigured(cluster, demo)) {
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

    zip.file('pom.xml', maven.generate(cluster));

    zip.file('README.txt', readme.generate());
    zip.file('jdbc-drivers/README.txt', readme.generateJDBC());

    if (_.isEmpty(data.pojos))
        data.pojos = java.pojos(cluster.caches);

    for (const pojo of data.pojos) {
        if (pojo.keyClass)
            zip.file(`${srcPath}/${pojo.keyType.replace(/\./g, '/')}.java`, pojo.keyClass);

        zip.file(`${srcPath}/${pojo.valueType.replace(/\./g, '/')}.java`, pojo.valueClass);
    }

    zip.generateAsync({
        type: 'blob',
        compression: 'DEFLATE',
        mimeType: 'application/octet-stream'
    }).then((blob) => postMessage(blob));
};
