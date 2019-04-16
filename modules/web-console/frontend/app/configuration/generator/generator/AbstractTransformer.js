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

import StringBuilder from './StringBuilder';

import IgniteConfigurationGenerator from './ConfigurationGenerator';

import IgniteClusterDefaults from './defaults/Cluster.service';
import IgniteCacheDefaults from './defaults/Cache.service';
import IgniteIGFSDefaults from './defaults/IGFS.service';

import JavaTypes from '../../../services/JavaTypes.service';
import {JavaTypesNonEnum} from '../JavaTypesNonEnum.service';

const clusterDflts = new IgniteClusterDefaults();
const cacheDflts = new IgniteCacheDefaults();
const igfsDflts = new IgniteIGFSDefaults();

export default class AbstractTransformer {
    static generator = IgniteConfigurationGenerator;
    static javaTypes = new JavaTypes();
    static javaTypesNonEnum = new JavaTypesNonEnum(clusterDflts, cacheDflts, igfsDflts, new JavaTypes());

    // Append comment with time stamp.
    static mainComment(sb, ...lines) {
        lines.push(sb.generatedBy());

        return this.commentBlock(sb, ...lines);
    }

    // Append line before and after property.
    static _emptyLineIfNeeded(sb, props, curIdx) {
        if (curIdx === props.length - 1)
            return;

        const cur = props[curIdx];

        // Empty line after.
        if (_.includes(['MAP', 'COLLECTION', 'ARRAY'], cur.clsName) || (cur.clsName === 'BEAN' && cur.value.isComplex()))
            return sb.emptyLine();

        const next = props[curIdx + 1];

        // Empty line before.
        if (_.includes(['MAP', 'COLLECTION', 'ARRAY'], next.clsName) || (next.clsName === 'BEAN' && next.value.isComplex()))
            return sb.emptyLine();
    }

    // Generate general section.
    static clusterGeneral(cluster, available) {
        return this.toSection(this.generator.clusterGeneral(cluster, available));
    }

    // Generate atomics group.
    static clusterAtomics(atomics, available) {
        return this.toSection(this.generator.clusterAtomics(atomics, available));
    }

    // Generate binary group.
    static clusterBinary(binary) {
        return this.toSection(this.generator.clusterBinary(binary));
    }

    // Generate cache key configurations.
    static clusterCacheKeyConfiguration(keyCfgs) {
        return this.toSection(this.generator.clusterCacheKeyConfiguration(keyCfgs));
    }

    // Generate client connector configuration.
    static clusterClientConnector(cluster, available) {
        return this.toSection(this.generator.clusterClientConnector(cluster, available));
    }

    // Generate collision group.
    static clusterCollision(collision) {
        return this.toSection(this.generator.clusterCollision(collision));
    }

    // Generate communication group.
    static clusterCommunication(cluster, available) {
        return this.toSection(this.generator.clusterCommunication(cluster, available));
    }

    // Generate REST access configuration.
    static clusterConnector(connector) {
        return this.toSection(this.generator.clusterConnector(connector));
    }

    // Generate deployment group.
    static clusterDeployment(cluster, available) {
        return this.toSection(this.generator.clusterDeployment(cluster, available));
    }

    // Generate discovery group.
    static clusterDiscovery(disco, available) {
        return this.toSection(this.generator.clusterDiscovery(disco, available));
    }

    // Generate events group.
    static clusterEvents(cluster, available) {
        return this.toSection(this.generator.clusterEvents(cluster, available));
    }

    // Generate failover group.
    static clusterFailover(cluster, available) {
        return this.toSection(this.generator.clusterFailover(cluster, available));
    }

    // Generate hadoop group.
    static clusterHadoop(hadoop) {
        return this.toSection(this.generator.clusterHadoop(hadoop));
    }

    // Generate cluster IGFSs group.
    static clusterIgfss(igfss, available) {
        return this.toSection(this.generator.clusterIgfss(igfss, available));
    }

    // Generate load balancing SPI group.
    static clusterLoadBalancing(cluster) {
        return this.toSection(this.generator.clusterLoadBalancing(cluster));
    }

    // Generate logger group.
    static clusterLogger(cluster) {
        return this.toSection(this.generator.clusterLogger(cluster));
    }

    // Generate memory configuration group.
    static clusterMemory(memoryConfiguration, available) {
        return this.toSection(this.generator.clusterMemory(memoryConfiguration, available));
    }

    // Generate memory configuration group.
    static clusterDataStorageConfiguration(cluster, available) {
        return this.toSection(this.generator.clusterDataStorageConfiguration(cluster, available));
    }

    // Generate marshaller group.
    static clusterMisc(cluster, available) {
        return this.toSection(this.generator.clusterMisc(cluster, available));
    }

    // Generate marshaller group.
    static clusterMvcc(cluster, available) {
        return this.toSection(this.generator.clusterMvcc(cluster, available));
    }

    // Generate encryption group.
    static clusterEncryption(encryption, available) {
        return this.toSection(this.generator.clusterEncryption(encryption, available));
    }

    // Generate marshaller group.
    static clusterMarshaller(cluster, available) {
        return this.toSection(this.generator.clusterMarshaller(cluster, available));
    }

    // Generate metrics group.
    static clusterMetrics(cluster, available) {
        return this.toSection(this.generator.clusterMetrics(cluster, available));
    }

    // Generate ODBC group.
    static clusterODBC(odbc, available) {
        return this.toSection(this.generator.clusterODBC(odbc, available));
    }

    // Generate cluster persistence store group.
    static clusterPersistence(persistence, available) {
        return this.toSection(this.generator.clusterPersistence(persistence, available));
    }

    // Generate cluster query group.
    static clusterQuery(cluster, available) {
        return this.toSection(this.generator.clusterQuery(cluster, available));
    }

    // Generate cache node filter group.
    static clusterServiceConfiguration(srvs, caches) {
        return this.toSection(this.generator.clusterServiceConfiguration(srvs, caches));
    }

    // Generate ssl group.
    static clusterSsl(cluster, available) {
        return this.toSection(this.generator.clusterSsl(cluster, available));
    }

    // Generate swap group.
    static clusterSwap(cluster) {
        return this.toSection(this.generator.clusterSwap(cluster));
    }

    // Generate time group.
    static clusterTime(cluster, available) {
        return this.toSection(this.generator.clusterTime(cluster, available));
    }

    // Generate thread pools group.
    static clusterPools(cluster, available) {
        return this.toSection(this.generator.clusterPools(cluster, available));
    }

    // Generate transactions group.
    static clusterTransactions(transactionConfiguration, available) {
        return this.toSection(this.generator.clusterTransactions(transactionConfiguration, available));
    }

    // Generate user attributes group.
    static clusterUserAttributes(cluster) {
        return this.toSection(this.generator.clusterUserAttributes(cluster));
    }

    // Generate IGFS general group.
    static igfsGeneral(igfs, available) {
        return this.toSection(this.generator.igfsGeneral(igfs, available));
    }

    // Generate IGFS secondary file system group.
    static igfsSecondFS(igfs) {
        return this.toSection(this.generator.igfsSecondFS(igfs));
    }

    // Generate IGFS IPC group.
    static igfsIPC(igfs) {
        return this.toSection(this.generator.igfsIPC(igfs));
    }

    // Generate IGFS fragmentizer group.
    static igfsFragmentizer(igfs) {
        return this.toSection(this.generator.igfsFragmentizer(igfs));
    }

    // Generate IGFS Dual mode group.
    static igfsDualMode(igfs) {
        return this.toSection(this.generator.igfsDualMode(igfs));
    }

    // Generate IGFS miscellaneous group.
    static igfsMisc(igfs, available) {
        return this.toSection(this.generator.igfsMisc(igfs, available));
    }

    // Generate cache general group.
    static cacheGeneral(cache, available) {
        return this.toSection(this.generator.cacheGeneral(cache, available));
    }

    // Generate cache memory group.
    static cacheAffinity(cache, available) {
        return this.toSection(this.generator.cacheAffinity(cache, available));
    }

    // Generate cache key configuration.
    static cacheKeyConfiguration(cache, available) {
        return this.toSection(this.generator.cacheKeyConfiguration(cache, available));
    }

    // Generate cache memory group.
    static cacheMemory(cache, available) {
        return this.toSection(this.generator.cacheMemory(cache, available));
    }

    // Generate cache queries & Indexing group.
    static cacheQuery(cache, domains, available) {
        return this.toSection(this.generator.cacheQuery(cache, domains, available));
    }

    // Generate cache store group.
    static cacheStore(cache, domains, available) {
        return this.toSection(this.generator.cacheStore(cache, domains, available));
    }

    // Generate cache concurrency control group.
    static cacheConcurrency(cache, available) {
        return this.toSection(this.generator.cacheConcurrency(cache, available));
    }

    // Generate cache misc group.
    static cacheMisc(cache, available) {
        return this.toSection(this.generator.cacheMisc(cache, available));
    }

    // Generate cache node filter group.
    static cacheNodeFilter(cache, igfss) {
        return this.toSection(this.generator.cacheNodeFilter(cache, igfss));
    }

    // Generate cache rebalance group.
    static cacheRebalance(cache) {
        return this.toSection(this.generator.cacheRebalance(cache));
    }

    // Generate server near cache group.
    static cacheNearServer(cache, available) {
        return this.toSection(this.generator.cacheNearServer(cache, available));
    }

    // Generate client near cache group.
    static cacheNearClient(cache, available) {
        return this.toSection(this.generator.cacheNearClient(cache, available));
    }

    // Generate cache statistics group.
    static cacheStatistics(cache) {
        return this.toSection(this.generator.cacheStatistics(cache));
    }

    // Generate caches configs.
    static clusterCaches(cluster, available, caches, igfss, client) {
        return this.toSection(this.generator.clusterCaches(cluster, caches, igfss, available, client));
    }

    // Generate caches configs.
    static clusterCheckpoint(cluster, available, caches) {
        return this.toSection(this.generator.clusterCheckpoint(cluster, available, caches));
    }

    // Generate domain model for general group.
    static domainModelGeneral(domain) {
        return this.toSection(this.generator.domainModelGeneral(domain));
    }

    // Generate domain model for query group.
    static domainModelQuery(domain, available) {
        return this.toSection(this.generator.domainModelQuery(domain, available));
    }

    // Generate domain model for store group.
    static domainStore(domain) {
        return this.toSection(this.generator.domainStore(domain));
    }

    /**
     * Check if configuration contains properties.
     *
     * @param {Bean} bean
     * @returns {Boolean}
     */
    static hasProperties(bean) {
        const searchProps = (prop) => {
            switch (prop.clsName) {
                case 'BEAN':
                    if (this.hasProperties(prop.value))
                        return true;

                    break;
                case 'ARRAY':
                case 'COLLECTION':
                    if (_.find(prop.items, (item) => this.hasProperties(item)))
                        return true;

                    break;
                case 'DATA_SOURCE':
                case 'PROPERTY':
                case 'PROPERTY_CHAR':
                    return true;
                default:
            }

            return false;
        };

        return _.isObject(bean) && (!!_.find(bean.arguments, searchProps) || !!_.find(bean.properties, searchProps));
    }

    /**
     * Collect datasource beans.
     *
     * @param {Bean} bean
     */
    static collectDataSources(bean) {
        const dataSources = _.reduce(bean.properties, (acc, prop) => {
            switch (prop.clsName.toUpperCase()) {
                case 'ARRAY':
                    if (this._isBean(prop.typeClsName))
                        _.forEach(prop.items, (item) => acc.push(...this.collectDataSources(item)));

                    break;
                case 'BEAN':
                    acc.push(...this.collectDataSources(prop.value));

                    break;
                case 'DATA_SOURCE':
                    acc.push(prop.value);

                    break;
                default:
            }

            return acc;
        }, []);

        return _.uniqBy(dataSources, (ds) => ds.id);
    }

    /**
     * Transform to section.
     *
     * @param cfg
     * @param sb
     * @return {StringBuilder}
     */
    static toSection(cfg, sb = new StringBuilder()) {
        this._setProperties(sb, cfg);

        return sb;
    }
}
