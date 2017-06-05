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

export default ['IgniteVersion', 'SpringTransformer', function(Version, spring) {
    const ctrl = this;

    delete ctrl.data;

    const targetSince = Version.available.bind(Version);

    // Setup generator.
    switch (ctrl.generator) {
        case 'igniteConfiguration':
            ctrl.generate = (cluster) => spring.cluster(cluster, Version.current, ctrl.client === 'true');

            break;
        case 'clusterCaches':
            ctrl.generate = (cluster, caches) => {
                const clusterCaches = _.reduce(caches, (acc, cache) => {
                    if (_.includes(cluster.caches, cache.value))
                        acc.push(cache.cache);

                    return acc;
                }, []);

                const cfg = spring.generator.clusterGeneral(cluster, targetSince);

                spring.generator.clusterCaches(cluster, clusterCaches, null, targetSince, false, cfg);

                return spring.toSection(cfg);
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

                return spring[ctrl.generator](cache, cacheDomains, targetSince);
            };

            break;
        case 'cacheNodeFilter':
            ctrl.generate = (cache, igfss) => {
                const cacheIgfss = _.reduce(igfss, (acc, igfs) => {
                    acc.push(igfs.igfs);

                    return acc;
                }, []);

                return spring.cacheNodeFilter(cache, cacheIgfss);
            };

            break;
        case 'clusterServiceConfiguration':
            ctrl.generate = (cluster, caches) => {
                const clusterCaches = _.reduce(caches, (acc, cache) => {
                    if (_.includes(cluster.caches, cache.value))
                        acc.push(cache.cache);

                    return acc;
                }, []);

                return spring.clusterServiceConfiguration(cluster.serviceConfigurations, clusterCaches);
            };

            break;
        case 'igfss':
            ctrl.generate = (cluster, igfss) => {
                const clusterIgfss = _.reduce(igfss, (acc, igfs) => {
                    if (_.includes(cluster.igfss, igfs.value))
                        acc.push(igfs.igfs);

                    return acc;
                }, []);

                return spring.clusterIgfss(clusterIgfss, targetSince);
            };

            break;
        default:
            ctrl.generate = (master) => spring[ctrl.generator](master, targetSince);
    }
}];
