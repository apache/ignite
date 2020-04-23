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

import {nonEmpty} from 'app/utils/lodashMixins';
import { EmptyBean, Bean } from './Beans';

/**
 * @param {import('app/services/JavaTypes.service').default} JavaTypes
 * @param {import('./defaults/Cluster.service').default} clusterDflts
 * @param {import('./defaults/Cache.service').default} cacheDflts
 */
export default function service(JavaTypes, clusterDflts, cacheDflts) {
    class PlatformGenerator {
        static igniteConfigurationBean(cluster) {
            return new Bean('Apache.Ignite.Core.IgniteConfiguration', 'cfg', cluster, clusterDflts);
        }

        static cacheConfigurationBean(cache) {
            return new Bean('Apache.Ignite.Core.Cache.Configuration.CacheConfiguration', 'ccfg', cache, cacheDflts);
        }

        /**
         * Function to generate ignite configuration.
         *
         * @param {Object} cluster Cluster to process.
         * @return {String} Generated ignite configuration.
         */
        static igniteConfiguration(cluster) {
            const cfg = this.igniteConfigurationBean(cluster);

            this.clusterAtomics(cluster.atomics, cfg);

            return cfg;
        }

        // Generate general section.
        static clusterGeneral(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            cfg.stringProperty('name', 'GridName')
                .stringProperty('localHost', 'Localhost');

            if (_.isNil(cluster.discovery))
                return cfg;

            const discovery = new Bean('Apache.Ignite.Core.Discovery.Tcp.TcpDiscoverySpi', 'discovery',
                cluster.discovery, clusterDflts.discovery);

            let ipFinder;

            switch (discovery.valueOf('kind')) {
                case 'Vm':
                    ipFinder = new Bean('Apache.Ignite.Core.Discovery.Tcp.Static.TcpDiscoveryStaticIpFinder',
                        'ipFinder', cluster.discovery.Vm, clusterDflts.discovery.Vm);

                    ipFinder.collectionProperty('addrs', 'addresses', cluster.discovery.Vm.addresses, 'ICollection');

                    break;
                case 'Multicast':
                    ipFinder = new Bean('Apache.Ignite.Core.Discovery.Tcp.Multicast.TcpDiscoveryMulticastIpFinder',
                        'ipFinder', cluster.discovery.Multicast, clusterDflts.discovery.Multicast);

                    ipFinder.stringProperty('MulticastGroup')
                        .intProperty('multicastPort', 'MulticastPort')
                        .intProperty('responseWaitTime', 'ResponseTimeout')
                        .intProperty('addressRequestAttempts', 'AddressRequestAttempts')
                        .stringProperty('localAddress', 'LocalAddress')
                        .collectionProperty('addrs', 'Endpoints', cluster.discovery.Multicast.addresses, 'ICollection');

                    break;
                default:
            }

            if (ipFinder)
                discovery.beanProperty('IpFinder', ipFinder);

            cfg.beanProperty('DiscoverySpi', discovery);


            return cfg;
        }

        static clusterAtomics(atomics, cfg = this.igniteConfigurationBean()) {
            const acfg = new Bean('Apache.Ignite.Core.DataStructures.Configuration.AtomicConfiguration', 'atomicCfg',
                atomics, clusterDflts.atomics);

            acfg.enumProperty('cacheMode', 'CacheMode')
                .intProperty('atomicSequenceReserveSize', 'AtomicSequenceReserveSize');

            if (acfg.valueOf('cacheMode') === 'PARTITIONED')
                acfg.intProperty('backups', 'Backups');

            if (acfg.isEmpty())
                return cfg;

            cfg.beanProperty('AtomicConfiguration', acfg);

            return cfg;
        }

        // Generate binary group.
        static clusterBinary(binary, cfg = this.igniteConfigurationBean()) {
            const binaryCfg = new Bean('Apache.Ignite.Core.Binary.BinaryConfiguration', 'binaryCfg',
                binary, clusterDflts.binary);

            binaryCfg.emptyBeanProperty('idMapper', 'DefaultIdMapper')
                .emptyBeanProperty('nameMapper', 'DefaultNameMapper')
                .emptyBeanProperty('serializer', 'DefaultSerializer');

            // const typeCfgs = [];
            //
            // _.forEach(binary.typeConfigurations, (type) => {
            //     const typeCfg = new MethodBean('Apache.Ignite.Core.Binary.BinaryTypeConfiguration',
            //         JavaTypes.toJavaName('binaryType', type.typeName), type, clusterDflts.binary.typeConfigurations);
            //
            //     typeCfg.stringProperty('typeName', 'TypeName')
            //         .emptyBeanProperty('idMapper', 'IdMapper')
            //         .emptyBeanProperty('nameMapper', 'NameMapper')
            //         .emptyBeanProperty('serializer', 'Serializer')
            //         .intProperty('enum', 'IsEnum');
            //
            //     if (typeCfg.nonEmpty())
            //         typeCfgs.push(typeCfg);
            // });
            //
            // binaryCfg.collectionProperty('types', 'TypeConfigurations', typeCfgs, 'ICollection',
            //     'Apache.Ignite.Core.Binary.BinaryTypeConfiguration');
            //
            // binaryCfg.boolProperty('compactFooter', 'CompactFooter');
            //
            // if (binaryCfg.isEmpty())
            //     return cfg;
            //
            // cfg.beanProperty('binaryConfiguration', binaryCfg);

            return cfg;
        }

        // Generate communication group.
        static clusterCommunication(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            const commSpi = new Bean('Apache.Ignite.Core.Communication.Tcp.TcpCommunicationSpi', 'communicationSpi',
                cluster.communication, clusterDflts.communication);

            commSpi.emptyBeanProperty('listener')
                .stringProperty('localAddress')
                .intProperty('localPort')
                .intProperty('localPortRange')
                // .intProperty('sharedMemoryPort')
                .intProperty('directBuffer')
                .intProperty('directSendBuffer')
                .intProperty('idleConnectionTimeout')
                .intProperty('connectTimeout')
                .intProperty('maxConnectTimeout')
                .intProperty('reconnectCount')
                .intProperty('socketSendBuffer')
                .intProperty('socketReceiveBuffer')
                .intProperty('messageQueueLimit')
                .intProperty('slowClientQueueLimit')
                .intProperty('tcpNoDelay')
                .intProperty('ackSendThreshold')
                .intProperty('unacknowledgedMessagesBufferSize')
                // .intProperty('socketWriteTimeout')
                .intProperty('selectorsCount');
            // .emptyBeanProperty('addressResolver');

            if (commSpi.nonEmpty())
                cfg.beanProperty('CommunicationSpi', commSpi);

            cfg.intProperty('networkTimeout', 'NetworkTimeout')
                .intProperty('networkSendRetryDelay')
                .intProperty('networkSendRetryCount');
            // .intProperty('discoveryStartupDelay');

            return cfg;
        }

        // Generate discovery group.
        static clusterDiscovery(discovery, cfg = this.igniteConfigurationBean()) {
            if (discovery) {
                let discoveryCfg = cfg.findProperty('discovery');

                if (_.isNil(discoveryCfg)) {
                    discoveryCfg = new Bean('Apache.Ignite.Core.Discovery.Tcp.TcpDiscoverySpi', 'discovery',
                        discovery, clusterDflts.discovery);
                }

                discoveryCfg.stringProperty('localAddress')
                    .intProperty('localPort')
                    .intProperty('localPortRange')
                    .intProperty('socketTimeout')
                    .intProperty('ackTimeout')
                    .intProperty('maxAckTimeout')
                    .intProperty('networkTimeout')
                    .intProperty('joinTimeout')
                    .intProperty('threadPriority')
                    .intProperty('heartbeatFrequency')
                    .intProperty('maxMissedHeartbeats')
                    .intProperty('maxMissedClientHeartbeats')
                    .intProperty('topHistorySize')
                    .intProperty('reconnectCount')
                    .intProperty('statisticsPrintFrequency')
                    .intProperty('ipFinderCleanFrequency')
                    .intProperty('forceServerMode')
                    .intProperty('clientReconnectDisabled');

                if (discoveryCfg.nonEmpty())
                    cfg.beanProperty('discoverySpi', discoveryCfg);
            }

            return cfg;
        }

        // Generate events group.
        static clusterEvents(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            if (nonEmpty(cluster.includeEventTypes))
                cfg.eventTypes('events', 'includeEventTypes', cluster.includeEventTypes);

            return cfg;
        }

        // Generate metrics group.
        static clusterMetrics(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            cfg.intProperty('metricsExpireTime')
                .intProperty('metricsHistorySize')
                .intProperty('metricsLogFrequency')
                .intProperty('metricsUpdateFrequency');

            return cfg;
        }

        // Generate transactions group.
        static clusterTransactions(transactionConfiguration, cfg = this.igniteConfigurationBean()) {
            const bean = new Bean('Apache.Ignite.Core.Transactions.TransactionConfiguration', 'TransactionConfiguration',
                transactionConfiguration, clusterDflts.transactionConfiguration);

            bean.enumProperty('defaultTxConcurrency', 'DefaultTransactionConcurrency')
                .enumProperty('defaultTxIsolation', 'DefaultTransactionIsolation')
                .intProperty('defaultTxTimeout', 'DefaultTimeout')
                .intProperty('pessimisticTxLogLinger', 'PessimisticTransactionLogLinger')
                .intProperty('pessimisticTxLogSize', 'PessimisticTransactionLogSize');

            if (bean.nonEmpty())
                cfg.beanProperty('transactionConfiguration', bean);

            return cfg;
        }

        // Generate user attributes group.
        static clusterUserAttributes(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            cfg.mapProperty('attributes', 'attributes', 'UserAttributes');

            return cfg;
        }

        static clusterCaches(cluster, caches, igfss, isSrvCfg, cfg = this.igniteConfigurationBean(cluster)) {
            // const cfg = this.clusterGeneral(cluster, cfg);
            //
            // if (nonEmpty(caches)) {
            //     const ccfgs = _.map(caches, (cache) => this.cacheConfiguration(cache));
            //
            //     cfg.collectionProperty('', '', ccfgs, );
            // }

            return this.clusterGeneral(cluster, cfg);
        }

        // Generate cache general group.
        static cacheGeneral(cache, ccfg = this.cacheConfigurationBean(cache)) {
            ccfg.stringProperty('name')
                .enumProperty('cacheMode')
                .enumProperty('atomicityMode');

            if (ccfg.valueOf('cacheMode') === 'PARTITIONED' && ccfg.valueOf('backups')) {
                ccfg.intProperty('backups')
                    .intProperty('readFromBackup');
            }

            ccfg.intProperty('copyOnRead');

            if (ccfg.valueOf('cacheMode') === 'PARTITIONED' && ccfg.valueOf('atomicityMode') === 'TRANSACTIONAL')
                ccfg.intProperty('isInvalidate', 'invalidate');

            return ccfg;
        }

        // Generate cache memory group.
        static cacheMemory(cache, available, ccfg = this.cacheConfigurationBean(cache)) {
            ccfg.enumProperty('memoryMode');

            if (ccfg.valueOf('memoryMode') !== 'OFFHEAP_VALUES')
                ccfg.intProperty('offHeapMaxMemory');

            // this._evictionPolicy(ccfg, available, false, cache.evictionPolicy, cacheDflts.evictionPolicy);

            ccfg.intProperty('startSize')
                .boolProperty('swapEnabled', 'EnableSwap');

            return ccfg;
        }

        // Generate cache queries & Indexing group.
        static cacheQuery(cache, domains, ccfg = this.cacheConfigurationBean(cache)) {
            ccfg.intProperty('sqlOnheapRowCacheSize')
                .intProperty('longQueryWarningTimeout');

            return ccfg;
        }

        // Generate cache store group.
        static cacheStore(cache, domains, ccfg = this.cacheConfigurationBean(cache)) {
            const kind = _.get(cache, 'cacheStoreFactory.kind');

            if (kind && cache.cacheStoreFactory[kind]) {
                let bean = null;

                const storeFactory = cache.cacheStoreFactory[kind];

                switch (kind) {
                    case 'CacheJdbcPojoStoreFactory':
                        bean = new Bean('org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory', 'cacheStoreFactory',
                            storeFactory);

                        const id = bean.valueOf('dataSourceBean');

                        bean.dataSource(id, 'dataSourceBean', this.dataSourceBean(id, storeFactory.dialect))
                            .beanProperty('dialect', new EmptyBean(this.dialectClsName(storeFactory.dialect)));

                        const setType = (typeBean, propName) => {
                            if (JavaTypes.nonBuiltInClass(typeBean.valueOf(propName)))
                                typeBean.stringProperty(propName);
                            else
                                typeBean.classProperty(propName);
                        };

                        const types = _.reduce(domains, (acc, domain) => {
                            if (_.isNil(domain.databaseTable))
                                return acc;

                            const typeBean = new Bean('org.apache.ignite.cache.store.jdbc.JdbcType', 'type',
                                _.merge({}, domain, {cacheName: cache.name}))
                                .stringProperty('cacheName');

                            setType(typeBean, 'keyType');
                            setType(typeBean, 'valueType');

                            this.domainStore(domain, typeBean);

                            acc.push(typeBean);

                            return acc;
                        }, []);

                        bean.arrayProperty('types', 'types', types, 'org.apache.ignite.cache.store.jdbc.JdbcType');

                        break;
                    case 'CacheJdbcBlobStoreFactory':
                        bean = new Bean('org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStoreFactory', 'cacheStoreFactory',
                            storeFactory);

                        if (bean.valueOf('connectVia') === 'DataSource')
                            bean.dataSource(bean.valueOf('dataSourceBean'), 'dataSourceBean', this.dialectClsName(storeFactory.dialect));
                        else {
                            ccfg.stringProperty('connectionUrl')
                                .stringProperty('user')
                                .property('password', `ds.${storeFactory.user}.password`, 'YOUR_PASSWORD');
                        }

                        bean.boolProperty('initSchema')
                            .stringProperty('createTableQuery')
                            .stringProperty('loadQuery')
                            .stringProperty('insertQuery')
                            .stringProperty('updateQuery')
                            .stringProperty('deleteQuery');

                        break;
                    case 'CacheHibernateBlobStoreFactory':
                        bean = new Bean('org.apache.ignite.cache.store.hibernate.CacheHibernateBlobStoreFactory',
                            'cacheStoreFactory', storeFactory);

                        bean.propsProperty('props', 'hibernateProperties');

                        break;
                    default:
                }

                if (bean)
                    ccfg.beanProperty('cacheStoreFactory', bean);
            }

            ccfg.boolProperty('storeKeepBinary')
                .boolProperty('loadPreviousValue')
                .boolProperty('readThrough')
                .boolProperty('writeThrough');

            if (ccfg.valueOf('writeBehindEnabled')) {
                ccfg.boolProperty('writeBehindEnabled')
                    .intProperty('writeBehindBatchSize')
                    .intProperty('writeBehindFlushSize')
                    .intProperty('writeBehindFlushFrequency')
                    .intProperty('writeBehindFlushThreadCount');
            }

            return ccfg;
        }

        // Generate cache concurrency control group.
        static cacheConcurrency(cache, ccfg = this.cacheConfigurationBean(cache)) {
            ccfg.intProperty('maxConcurrentAsyncOperations')
                .intProperty('defaultLockTimeout')
                .enumProperty('atomicWriteOrderMode')
                .enumProperty('writeSynchronizationMode');

            return ccfg;
        }

        // Generate cache node filter group.
        static cacheNodeFilter(cache, igfss, ccfg = this.cacheConfigurationBean(cache)) {
            const kind = _.get(cache, 'nodeFilter.kind');

            if (kind && cache.nodeFilter[kind]) {
                let bean = null;

                switch (kind) {
                    case 'IGFS':
                        const foundIgfs = _.find(igfss, (igfs) => igfs._id === cache.nodeFilter.IGFS.igfs);

                        if (foundIgfs) {
                            bean = new Bean('org.apache.ignite.internal.processors.igfs.IgfsNodePredicate', 'nodeFilter', foundIgfs)
                                .stringConstructorArgument('name');
                        }

                        break;
                    case 'Custom':
                        bean = new Bean(cache.nodeFilter.Custom.className, 'nodeFilter');

                        break;
                    default:
                        return ccfg;
                }

                if (bean)
                    ccfg.beanProperty('nodeFilter', bean);
            }

            return ccfg;
        }

        // Generate cache rebalance group.
        static cacheRebalance(cache, ccfg = this.cacheConfigurationBean(cache)) {
            if (ccfg.valueOf('cacheMode') !== 'LOCAL') {
                ccfg.enumProperty('rebalanceMode')
                    .intProperty('rebalanceThreadPoolSize')
                    .intProperty('rebalanceBatchSize')
                    .intProperty('rebalanceBatchesPrefetchCount')
                    .intProperty('rebalanceOrder')
                    .intProperty('rebalanceDelay')
                    .intProperty('rebalanceTimeout')
                    .intProperty('rebalanceThrottle');
            }

            if (ccfg.includes('igfsAffinnityGroupSize')) {
                const bean = new Bean('org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper', 'affinityMapper', cache)
                    .intConstructorArgument('igfsAffinnityGroupSize');

                ccfg.beanProperty('affinityMapper', bean);
            }

            return ccfg;
        }

        // Generate server near cache group.
        static cacheServerNearCache(cache, ccfg = this.cacheConfigurationBean(cache)) {
            if (cache.cacheMode === 'PARTITIONED' && cache.nearCacheEnabled) {
                const bean = new Bean('org.apache.ignite.configuration.NearCacheConfiguration', 'nearConfiguration',
                    cache.nearConfiguration, {nearStartSize: 375000});

                bean.intProperty('nearStartSize');

                this._evictionPolicy(bean, true,
                    bean.valueOf('nearEvictionPolicy'), cacheDflts.evictionPolicy);

                ccfg.beanProperty('nearConfiguration', bean);
            }

            return ccfg;
        }

        // Generate cache statistics group.
        static cacheStatistics(cache, ccfg = this.cacheConfigurationBean(cache)) {
            ccfg.boolProperty('statisticsEnabled')
                .boolProperty('managementEnabled');

            return ccfg;
        }

        static cacheConfiguration(cache, ccfg = this.cacheConfigurationBean(cache)) {
            this.cacheGeneral(cache, ccfg);
            this.cacheMemory(cache, ccfg);
            this.cacheQuery(cache, cache.domains, ccfg);
            this.cacheStore(cache, cache.domains, ccfg);

            const igfs = _.get(cache, 'nodeFilter.IGFS.instance');
            this.cacheNodeFilter(cache, igfs ? [igfs] : [], ccfg);
            this.cacheConcurrency(cache, ccfg);
            this.cacheRebalance(cache, ccfg);
            this.cacheServerNearCache(cache, ccfg);
            this.cacheStatistics(cache, ccfg);
            // this.cacheDomains(cache.domains, cfg);

            return ccfg;
        }
    }

    return PlatformGenerator;
}

service.$inject = ['JavaTypes', 'igniteClusterPlatformDefaults', 'igniteCachePlatformDefaults'];
