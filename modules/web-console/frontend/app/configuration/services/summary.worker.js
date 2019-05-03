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

import IgniteMavenGenerator from '../generator/generator/Maven.service';
import IgniteDockerGenerator from '../generator/generator/Docker.service';
import IgniteReadmeGenerator from '../generator/generator/Readme.service';
import IgnitePropertiesGenerator from '../generator/generator/Properties.service';
import IgniteConfigurationGenerator from '../generator/generator/ConfigurationGenerator';

import IgniteJavaTransformer from '../generator/generator/JavaTransformer.service';
import IgniteSpringTransformer from '../generator/generator/SpringTransformer.service';

import {nonEmpty, nonNil} from 'app/utils/lodashMixins';
import get from 'lodash/get';
import filter from 'lodash/filter';
import isEmpty from 'lodash/isEmpty';

const maven = new IgniteMavenGenerator();
const docker = new IgniteDockerGenerator();
const readme = new IgniteReadmeGenerator();
const properties = new IgnitePropertiesGenerator();

const java = IgniteJavaTransformer;
const spring = IgniteSpringTransformer;

const generator = IgniteConfigurationGenerator;

const escapeFileName = (name) => name.replace(/[\\\/*\"\[\],\.:;|=<>?]/g, '-').replace(/ /g, '_');

const kubernetesConfig = (cluster) => {
    if (!cluster.discovery.Kubernetes)
        cluster.discovery.Kubernetes = { serviceName: 'ignite' };

    return `apiVersion: v1\n\
kind: Service\n\
metadata:\n\
  # Name of Ignite Service used by Kubernetes IP finder for IP addresses lookup.\n\
  name: ${ cluster.discovery.Kubernetes.serviceName || 'ignite' }\n\
spec:\n\
  clusterIP: None # custom value.\n\
  ports:\n\
    - port: 9042 # custom value.\n\
  selector:\n\
    # Must be equal to one of the labels set in Ignite pods'\n\
    # deployement configuration.\n\
    app: ${ cluster.discovery.Kubernetes.serviceName || 'ignite' }`;
};

// eslint-disable-next-line no-undef
onmessage = function(e) {
    const {cluster, data, demo, targetVer} = e.data;

    const zip = new JSZip();

    if (!data.docker)
        data.docker = docker.generate(cluster, targetVer);

    zip.file('Dockerfile', data.docker);
    zip.file('.dockerignore', docker.ignoreFile());

    const cfg = generator.igniteConfiguration(cluster, targetVer, false);
    const clientCfg = generator.igniteConfiguration(cluster, targetVer, true);
    const clientNearCaches = filter(cluster.caches, (cache) =>
        cache.cacheMode === 'PARTITIONED' && get(cache, 'clientNearConfiguration.enabled'));

    const secProps = properties.generate(cfg);

    if (secProps)
        zip.file('src/main/resources/secret.properties', secProps);

    const srcPath = 'src/main/java';
    const resourcesPath = 'src/main/resources';

    const serverXml = `${escapeFileName(cluster.name)}-server.xml`;
    const clientXml = `${escapeFileName(cluster.name)}-client.xml`;

    const metaPath = `${resourcesPath}/META-INF`;

    if (cluster.discovery.kind === 'Kubernetes')
        zip.file(`${metaPath}/ignite-service.yaml`, kubernetesConfig(cluster));

    zip.file(`${metaPath}/${serverXml}`, spring.igniteConfiguration(cfg, targetVer).asString());
    zip.file(`${metaPath}/${clientXml}`, spring.igniteConfiguration(clientCfg, targetVer, clientNearCaches).asString());

    const cfgPath = `${srcPath}/config`;

    zip.file(`${cfgPath}/ServerConfigurationFactory.java`, java.igniteConfiguration(cfg, targetVer, 'config', 'ServerConfigurationFactory').asString());
    zip.file(`${cfgPath}/ClientConfigurationFactory.java`, java.igniteConfiguration(clientCfg, targetVer, 'config', 'ClientConfigurationFactory', clientNearCaches).asString());

    if (java.isDemoConfigured(cluster, demo)) {
        zip.file(`${srcPath}/demo/DemoStartup.java`, java.nodeStartup(cluster, 'demo.DemoStartup',
            'ServerConfigurationFactory.createConfiguration()', 'config.ServerConfigurationFactory'));
    }

    // Generate loader for caches with configured store.
    const cachesToLoad = filter(cluster.caches, (cache) => nonNil(_.get(cache, 'cacheStoreFactory.kind')));

    if (nonEmpty(cachesToLoad))
        zip.file(`${srcPath}/load/LoadCaches.java`, java.loadCaches(cachesToLoad, 'load', 'LoadCaches', `"${clientXml}"`));

    const startupPath = `${srcPath}/startup`;

    zip.file(`${startupPath}/ServerNodeSpringStartup.java`, java.nodeStartup(cluster, 'startup.ServerNodeSpringStartup', `"${serverXml}"`));
    zip.file(`${startupPath}/ClientNodeSpringStartup.java`, java.nodeStartup(cluster, 'startup.ClientNodeSpringStartup', `"${clientXml}"`));

    zip.file(`${startupPath}/ServerNodeCodeStartup.java`, java.nodeStartup(cluster, 'startup.ServerNodeCodeStartup',
        'ServerConfigurationFactory.createConfiguration()', 'config.ServerConfigurationFactory'));
    zip.file(`${startupPath}/ClientNodeCodeStartup.java`, java.nodeStartup(cluster, 'startup.ClientNodeCodeStartup',
        'ClientConfigurationFactory.createConfiguration()', 'config.ClientConfigurationFactory', clientNearCaches));

    zip.file('pom.xml', maven.generate(cluster, targetVer));

    zip.file('README.txt', readme.generate());
    zip.file('jdbc-drivers/README.txt', readme.generateJDBC());

    if (isEmpty(data.pojos))
        data.pojos = java.pojos(cluster.caches, true);

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
