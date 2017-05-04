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

export default ['$scope', 'JavaTransformer', function($scope, java) {
    const ctrl = this;

    delete ctrl.data;

    const client = ctrl.client === 'true';

    // Setup generator.
    switch (ctrl.generator) {
        case 'igniteConfiguration':
            const clsName = client ? 'ClientConfigurationFactory' : 'ServerConfigurationFactory';

            ctrl.generate = (cluster) => java.cluster(cluster, 'config', clsName, client);

            break;
        case 'clusterCaches':
            ctrl.generate = (cluster, caches) => {
                const clusterCaches = _.reduce(caches, (acc, cache) => {
                    if (_.includes(cluster.caches, cache.value))
                        acc.push(cache.cache);

                    return acc;
                }, []);

                const cfg = java.generator.clusterGeneral(cluster);

                java.generator.clusterCaches(cluster, clusterCaches, null, false, cfg);

                return java.toSection(cfg);
            };

            break;
        case 'cacheStore':
        case 'cacheQuery':
            ctrl.generate = (cache, domains) => {
                const cacheDomains = _.reduce(domains, (acc, domain) => {
                    if (_.includes(cache.domains, domain.value))
                        acc.push(domain.meta);

                    return acc;
                }, []);

                return java[ctrl.generator](cache, cacheDomains);
            };

            break;
        case 'cacheNodeFilter':
            ctrl.generate = (cache, igfss) => {
                const cacheIgfss = _.reduce(igfss, (acc, igfs) => {
                    acc.push(igfs.igfs);

                    return acc;
                }, []);

                return java.cacheNodeFilter(cache, cacheIgfss);
            };

            break;
        case 'igfss':
            ctrl.generate = (cluster, igfss) => {
                const clusterIgfss = _.reduce(igfss, (acc, igfs) => {
                    if (_.includes(cluster.igfss, igfs.value))
                        acc.push(igfs.igfs);

                    return acc;
                }, []);

                return java.clusterIgfss(clusterIgfss);
            };

            break;
        default:
            ctrl.generate = (master, detail) => java[ctrl.generator](master, detail);
    }
}];
