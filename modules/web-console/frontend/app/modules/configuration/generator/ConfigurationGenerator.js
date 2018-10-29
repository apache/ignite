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

import DFLT_DIALECTS from 'app/data/dialects.json';

import { EmptyBean, Bean } from './Beans';

import IgniteClusterDefaults from './defaults/Cluster.service';
import IgniteEventGroups from './defaults/Event-groups.service';
import IgniteCacheDefaults from './defaults/Cache.service';
import IgniteIGFSDefaults from './defaults/IGFS.service';

import JavaTypes from '../../../services/JavaTypes.service';
import VersionService from 'app/services/Version.service';

import isNil from 'lodash/isNil';
import {nonNil, nonEmpty} from 'app/utils/lodashMixins';

const clusterDflts = new IgniteClusterDefaults();
const cacheDflts = new IgniteCacheDefaults();
const igfsDflts = new IgniteIGFSDefaults();
const javaTypes = new JavaTypes(clusterDflts, cacheDflts, igfsDflts);
const versionService = new VersionService();

export default class IgniteConfigurationGenerator {
    static eventGrps = new IgniteEventGroups();

    static igniteConfigurationBean(cluster) {
        return new Bean('org.apache.ignite.configuration.IgniteConfiguration', 'cfg', cluster, clusterDflts);
    }

    static igfsConfigurationBean(igfs) {
        return new Bean('org.apache.ignite.configuration.FileSystemConfiguration', 'igfs', igfs, igfsDflts);
    }

    static cacheConfigurationBean(cache) {
        return new Bean('org.apache.ignite.configuration.CacheConfiguration', 'ccfg', cache, cacheDflts);
    }

    static domainConfigurationBean(domain) {
        return new Bean('org.apache.ignite.cache.QueryEntity', 'qryEntity', domain, cacheDflts);
    }

    static domainJdbcTypeBean(domain) {
        return new Bean('org.apache.ignite.cache.store.jdbc.JdbcType', 'type', domain);
    }

    static discoveryConfigurationBean(discovery) {
        return new Bean('org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi', 'discovery', discovery, clusterDflts.discovery);
    }

    /**
     * Function to generate ignite configuration.
     *
     * @param {Object} cluster Cluster to process.
     * @param {Object} targetVer Target version of configuration.
     * @param {Boolean} client Is client configuration.
     * @return {Bean} Generated ignite configuration.
     */
    static igniteConfiguration(cluster, targetVer, client) {
        const available = versionService.since.bind(versionService, targetVer.ignite);

        const cfg = this.igniteConfigurationBean(cluster);

        this.clusterGeneral(cluster, available, cfg, client);
        this.clusterAtomics(cluster.atomicConfiguration, available, cfg);
        this.clusterBinary(cluster.binaryConfiguration, cfg);
        this.clusterCacheKeyConfiguration(cluster.cacheKeyConfiguration, cfg);
        this.clusterCheckpoint(cluster, available, cluster.caches, cfg);

        if (available('2.3.0'))
            this.clusterClientConnector(cluster, available, cfg);

        this.clusterCollision(cluster.collision, cfg);
        this.clusterCommunication(cluster, available, cfg);
        this.clusterConnector(cluster.connector, cfg);

        // Since ignite 2.3
        if (available('2.3.0'))
            this.clusterDataStorageConfiguration(cluster.dataStorageConfiguration, available, cfg);

        this.clusterDeployment(cluster, available, cfg);
        this.clusterEvents(cluster, available, cfg);
        this.clusterFailover(cluster, available, cfg);
        this.clusterHadoop(cluster.hadoopConfiguration, cfg);
        this.clusterLoadBalancing(cluster, cfg);
        this.clusterLogger(cluster.logger, cfg);
        this.clusterMarshaller(cluster, available, cfg);

        // Since ignite 2.0 and deprecated in ignite 2.3
        if (available(['2.0.0', '2.3.0']))
            this.clusterMemory(cluster.memoryConfiguration, available, cfg);

        this.clusterMisc(cluster, available, cfg);
        this.clusterMetrics(cluster, available, cfg);
        this.clusterODBC(cluster.odbc, available, cfg);

        // Since ignite 2.1 deprecated in ignite 2.3
        if (available(['2.1.0', '2.3.0']))
            this.clusterPersistence(cluster.persistenceStoreConfiguration, available, cfg);

        if (available(['2.1.0', '2.3.0']))
            this.clusterQuery(cluster, available, cfg);

        this.clusterServiceConfiguration(cluster.serviceConfigurations, cluster.caches, cfg);
        this.clusterSsl(cluster, cfg);

        // Deprecated in ignite 2.0
        if (available(['1.0.0', '2.0.0']))
            this.clusterSwap(cluster, cfg);

        this.clusterPools(cluster, available, cfg);
        this.clusterTime(cluster, available, cfg);
        this.clusterTransactions(cluster.transactionConfiguration, cfg);
        this.clusterUserAttributes(cluster, cfg);

        this.clusterCaches(cluster, cluster.caches, cluster.igfss, available, client, cfg);

        if (!client)
            this.clusterIgfss(cluster.igfss, available, cfg);

        return cfg;
    }

    static dialectClsName(dialect) {
        return DFLT_DIALECTS[dialect] || 'Unknown database: ' + (dialect || 'Choose JDBC dialect');
    }

    static dataSourceBean(id, dialect) {
        let dsBean;

        switch (dialect) {
            case 'Generic':
                dsBean = new Bean('com.mchange.v2.c3p0.ComboPooledDataSource', id, {})
                    .property('jdbcUrl', `${id}.jdbc.url`, 'jdbc:your_database');

                break;
            case 'Oracle':
                dsBean = new Bean('oracle.jdbc.pool.OracleDataSource', id, {})
                    .property('URL', `${id}.jdbc.url`, 'jdbc:oracle:thin:@[host]:[port]:[database]');

                break;
            case 'DB2':
                dsBean = new Bean('com.ibm.db2.jcc.DB2DataSource', id, {})
                    .property('serverName', `${id}.jdbc.server_name`, 'YOUR_DATABASE_SERVER_NAME')
                    .propertyInt('portNumber', `${id}.jdbc.port_number`, 'YOUR_JDBC_PORT_NUMBER')
                    .property('databaseName', `${id}.jdbc.database_name`, 'YOUR_DATABASE_NAME')
                    .propertyInt('driverType', `${id}.jdbc.driver_type`, 'YOUR_JDBC_DRIVER_TYPE');

                break;
            case 'SQLServer':
                dsBean = new Bean('com.microsoft.sqlserver.jdbc.SQLServerDataSource', id, {})
                    .property('URL', `${id}.jdbc.url`, 'jdbc:sqlserver://[host]:[port][;databaseName=database]');

                break;
            case 'MySQL':
                dsBean = new Bean('com.mysql.jdbc.jdbc2.optional.MysqlDataSource', id, {})
                    .property('URL', `${id}.jdbc.url`, 'jdbc:mysql://[host]:[port]/[database]');

                break;
            case 'PostgreSQL':
                dsBean = new Bean('org.postgresql.ds.PGPoolingDataSource', id, {})
                    .property('url', `${id}.jdbc.url`, 'jdbc:postgresql://[host]:[port]/[database]');

                break;
            case 'H2':
                dsBean = new Bean('org.h2.jdbcx.JdbcDataSource', id, {})
                    .property('URL', `${id}.jdbc.url`, 'jdbc:h2:tcp://[host]/[database]');

                break;
            default:
        }

        if (dsBean) {
            dsBean.property('user', `${id}.jdbc.username`, 'YOUR_USER_NAME')
                .property('password', `${id}.jdbc.password`, 'YOUR_PASSWORD');
        }

        return dsBean;
    }

    // Generate general section.
    static clusterGeneral(cluster, available, cfg = this.igniteConfigurationBean(cluster), client = false) {
        if (client)
            cfg.prop('boolean', 'clientMode', true);

        if (available('2.0.0'))
            cfg.stringProperty('name', 'igniteInstanceName');
        else
            cfg.stringProperty('name', 'gridName');

        cfg.stringProperty('localHost');

        if (isNil(cluster.discovery))
            return cfg;

        const discovery = new Bean('org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi', 'discovery',
            cluster.discovery, clusterDflts.discovery);

        let ipFinder;

        switch (discovery.valueOf('kind')) {
            case 'Vm':
                ipFinder = new Bean('org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder',
                    'ipFinder', cluster.discovery.Vm, clusterDflts.discovery.Vm);

                ipFinder.collectionProperty('addrs', 'addresses', cluster.discovery.Vm.addresses);

                break;
            case 'Multicast':
                ipFinder = new Bean('org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder',
                    'ipFinder', cluster.discovery.Multicast, clusterDflts.discovery.Multicast);

                ipFinder.stringProperty('multicastGroup')
                    .intProperty('multicastPort')
                    .intProperty('responseWaitTime')
                    .intProperty('addressRequestAttempts')
                    .stringProperty('localAddress')
                    .collectionProperty('addrs', 'addresses', cluster.discovery.Multicast.addresses);

                break;
            case 'S3':
                ipFinder = new Bean('org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinder',
                    'ipFinder', cluster.discovery.S3, clusterDflts.discovery.S3);

                ipFinder.stringProperty('bucketName');

                if (available('2.4.0')) {
                    ipFinder.stringProperty('bucketEndpoint')
                        .stringProperty('SSEAlgorithm');
                }

                break;
            case 'Cloud':
                ipFinder = new Bean('org.apache.ignite.spi.discovery.tcp.ipfinder.cloud.TcpDiscoveryCloudIpFinder',
                    'ipFinder', cluster.discovery.Cloud, clusterDflts.discovery.Cloud);

                ipFinder.stringProperty('credential')
                    .pathProperty('credentialPath')
                    .stringProperty('identity')
                    .stringProperty('provider')
                    .collectionProperty('regions', 'regions', cluster.discovery.Cloud.regions)
                    .collectionProperty('zones', 'zones', cluster.discovery.Cloud.zones);

                break;
            case 'GoogleStorage':
                ipFinder = new Bean('org.apache.ignite.spi.discovery.tcp.ipfinder.gce.TcpDiscoveryGoogleStorageIpFinder',
                    'ipFinder', cluster.discovery.GoogleStorage, clusterDflts.discovery.GoogleStorage);

                ipFinder.stringProperty('projectName')
                    .stringProperty('bucketName')
                    .pathProperty('serviceAccountP12FilePath')
                    .stringProperty('serviceAccountId');

                break;
            case 'Jdbc':
                ipFinder = new Bean('org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc.TcpDiscoveryJdbcIpFinder',
                    'ipFinder', cluster.discovery.Jdbc, clusterDflts.discovery.Jdbc);

                ipFinder.intProperty('initSchema');

                if (ipFinder.includes('dataSourceBean', 'dialect')) {
                    const id = ipFinder.valueOf('dataSourceBean');

                    ipFinder.dataSource(id, 'dataSource', this.dataSourceBean(id, ipFinder.valueOf('dialect')));
                }

                break;
            case 'SharedFs':
                ipFinder = new Bean('org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinder',
                    'ipFinder', cluster.discovery.SharedFs, clusterDflts.discovery.SharedFs);

                ipFinder.pathProperty('path');

                break;
            case 'ZooKeeper':
                const src = cluster.discovery.ZooKeeper;
                const dflt = clusterDflts.discovery.ZooKeeper;

                ipFinder = new Bean('org.apache.ignite.spi.discovery.tcp.ipfinder.zk.TcpDiscoveryZookeeperIpFinder',
                    'ipFinder', src, dflt);

                ipFinder.emptyBeanProperty('curator')
                    .stringProperty('zkConnectionString');

                const kind = _.get(src, 'retryPolicy.kind');

                if (kind) {
                    const policy = src.retryPolicy;

                    let retryPolicyBean;

                    switch (kind) {
                        case 'ExponentialBackoff':
                            retryPolicyBean = new Bean('org.apache.curator.retry.ExponentialBackoffRetry', null,
                                policy.ExponentialBackoff, dflt.ExponentialBackoff)
                                .intConstructorArgument('baseSleepTimeMs')
                                .intConstructorArgument('maxRetries')
                                .intConstructorArgument('maxSleepMs');

                            break;
                        case 'BoundedExponentialBackoff':
                            retryPolicyBean = new Bean('org.apache.curator.retry.BoundedExponentialBackoffRetry',
                                null, policy.BoundedExponentialBackoff, dflt.BoundedExponentialBackoffRetry)
                                .intConstructorArgument('baseSleepTimeMs')
                                .intConstructorArgument('maxSleepTimeMs')
                                .intConstructorArgument('maxRetries');

                            break;
                        case 'UntilElapsed':
                            retryPolicyBean = new Bean('org.apache.curator.retry.RetryUntilElapsed', null,
                                policy.UntilElapsed, dflt.UntilElapsed)
                                .intConstructorArgument('maxElapsedTimeMs')
                                .intConstructorArgument('sleepMsBetweenRetries');

                            break;

                        case 'NTimes':
                            retryPolicyBean = new Bean('org.apache.curator.retry.RetryNTimes', null,
                                policy.NTimes, dflt.NTimes)
                                .intConstructorArgument('n')
                                .intConstructorArgument('sleepMsBetweenRetries');

                            break;
                        case 'OneTime':
                            retryPolicyBean = new Bean('org.apache.curator.retry.RetryOneTime', null,
                                policy.OneTime, dflt.OneTime)
                                .intConstructorArgument('sleepMsBetweenRetry');

                            break;
                        case 'Forever':
                            retryPolicyBean = new Bean('org.apache.curator.retry.RetryForever', null,
                                policy.Forever, dflt.Forever)
                                .intConstructorArgument('retryIntervalMs');

                            break;
                        case 'Custom':
                            const className = _.get(policy, 'Custom.className');

                            if (nonEmpty(className))
                                retryPolicyBean = new EmptyBean(className);

                            break;
                        default:
                            // No-op.
                    }

                    if (retryPolicyBean)
                        ipFinder.beanProperty('retryPolicy', retryPolicyBean);
                }

                ipFinder.pathProperty('basePath')
                    .stringProperty('serviceName')
                    .boolProperty('allowDuplicateRegistrations');

                break;

            case 'Kubernetes':
                ipFinder = new Bean('org.apache.ignite.spi.discovery.tcp.ipfinder.kubernetes.TcpDiscoveryKubernetesIpFinder',
                    'ipFinder', cluster.discovery.Kubernetes, clusterDflts.discovery.Kubernetes);

                ipFinder.stringProperty('serviceName')
                    .stringProperty('namespace')
                    .stringProperty('masterUrl')
                    .pathProperty('accountToken');

                break;

            default:
                // No-op.
        }

        if (ipFinder)
            discovery.beanProperty('ipFinder', ipFinder);

        this.clusterDiscovery(cluster.discovery, available, cfg, discovery);

        return cfg;
    }

    static igfsDataCache(igfs, available) {
        return this.cacheConfiguration({
            name: igfs.name + '-data',
            cacheMode: 'PARTITIONED',
            atomicityMode: 'TRANSACTIONAL',
            writeSynchronizationMode: 'FULL_SYNC',
            backups: 0,
            igfsAffinnityGroupSize: igfs.affinnityGroupSize || 512
        }, available);
    }

    static igfsMetaCache(igfs, available) {
        return this.cacheConfiguration({
            name: igfs.name + '-meta',
            cacheMode: 'REPLICATED',
            atomicityMode: 'TRANSACTIONAL',
            writeSynchronizationMode: 'FULL_SYNC'
        }, available);
    }

    static clusterCaches(cluster, caches, igfss, available, client, cfg = this.igniteConfigurationBean(cluster)) {
        const ccfgs = _.map(caches, (cache) => this.cacheConfiguration(cache, available));

        if (!client) {
            _.forEach(igfss, (igfs) => {
                ccfgs.push(this.igfsDataCache(igfs, available));
                ccfgs.push(this.igfsMetaCache(igfs, available));
            });
        }

        cfg.varArgProperty('ccfgs', 'cacheConfiguration', ccfgs, 'org.apache.ignite.configuration.CacheConfiguration');

        return cfg;
    }

    // Generate atomics group.
    static clusterAtomics(atomics, available, cfg = this.igniteConfigurationBean()) {
        const acfg = new Bean('org.apache.ignite.configuration.AtomicConfiguration', 'atomicCfg',
            atomics, clusterDflts.atomics);

        acfg.enumProperty('cacheMode')
            .intProperty('atomicSequenceReserveSize');

        if (acfg.valueOf('cacheMode') === 'PARTITIONED')
            acfg.intProperty('backups');

        if (available('2.1.0') && nonNil(atomics))
            this.affinity(atomics.affinity, acfg);

        if (acfg.isEmpty())
            return cfg;

        cfg.beanProperty('atomicConfiguration', acfg);

        return cfg;
    }

    // Generate binary group.
    static clusterBinary(binary, cfg = this.igniteConfigurationBean()) {
        const binaryCfg = new Bean('org.apache.ignite.configuration.BinaryConfiguration', 'binaryCfg',
            binary, clusterDflts.binary);

        binaryCfg.emptyBeanProperty('idMapper')
            .emptyBeanProperty('nameMapper')
            .emptyBeanProperty('serializer');

        const typeCfgs = [];

        _.forEach(binary.typeConfigurations, (type) => {
            const typeCfg = new Bean('org.apache.ignite.binary.BinaryTypeConfiguration',
                javaTypes.toJavaName('binaryType', type.typeName), type, clusterDflts.binary.typeConfigurations);

            typeCfg.stringProperty('typeName')
                .emptyBeanProperty('idMapper')
                .emptyBeanProperty('nameMapper')
                .emptyBeanProperty('serializer')
                .intProperty('enum');

            if (typeCfg.nonEmpty())
                typeCfgs.push(typeCfg);
        });

        binaryCfg.collectionProperty('types', 'typeConfigurations', typeCfgs, 'org.apache.ignite.binary.BinaryTypeConfiguration')
            .boolProperty('compactFooter');

        if (binaryCfg.isEmpty())
            return cfg;

        cfg.beanProperty('binaryConfiguration', binaryCfg);

        return cfg;
    }

    // Generate cache key configurations.
    static clusterCacheKeyConfiguration(keyCfgs, cfg = this.igniteConfigurationBean()) {
        const items = _.reduce(keyCfgs, (acc, keyCfg) => {
            if (keyCfg.typeName && keyCfg.affinityKeyFieldName) {
                acc.push(new Bean('org.apache.ignite.cache.CacheKeyConfiguration', null, keyCfg)
                    .stringConstructorArgument('typeName')
                    .stringConstructorArgument('affinityKeyFieldName'));
            }

            return acc;
        }, []);

        if (_.isEmpty(items))
            return cfg;

        cfg.arrayProperty('cacheKeyConfiguration', 'cacheKeyConfiguration', items,
            'org.apache.ignite.cache.CacheKeyConfiguration');

        return cfg;
    }

    // Generate checkpoint configurations.
    static clusterCheckpoint(cluster, available, caches, cfg = this.igniteConfigurationBean()) {
        const cfgs = _.filter(_.map(cluster.checkpointSpi, (spi) => {
            switch (_.get(spi, 'kind')) {
                case 'FS':
                    const fsBean = new Bean('org.apache.ignite.spi.checkpoint.sharedfs.SharedFsCheckpointSpi',
                        'checkpointSpiFs', spi.FS);

                    fsBean.collectionProperty('directoryPaths', 'directoryPaths', _.get(spi, 'FS.directoryPaths'))
                        .emptyBeanProperty('checkpointListener');

                    return fsBean;

                case 'Cache':
                    const cacheBean = new Bean('org.apache.ignite.spi.checkpoint.cache.CacheCheckpointSpi',
                        'checkpointSpiCache', spi.Cache);

                    const curCache = _.get(spi, 'Cache.cache');

                    const cache = _.find(caches, (c) => curCache && (c._id === curCache || _.get(c, 'cache._id') === curCache));

                    if (cache)
                        cacheBean.prop('java.lang.String', 'cacheName', cache.name || cache.cache.name);

                    cacheBean.stringProperty('cacheName')
                        .emptyBeanProperty('checkpointListener');

                    return cacheBean;

                case 'S3':
                    const s3Bean = new Bean('org.apache.ignite.spi.checkpoint.s3.S3CheckpointSpi',
                        'checkpointSpiS3', spi.S3, clusterDflts.checkpointSpi.S3);

                    let credentialsBean = null;

                    switch (_.get(spi.S3, 'awsCredentials.kind')) {
                        case 'Basic':
                            credentialsBean = new Bean('com.amazonaws.auth.BasicAWSCredentials', 'awsCredentials', {});

                            credentialsBean.propertyConstructorArgument('checkpoint.s3.credentials.accessKey', 'YOUR_S3_ACCESS_KEY')
                                .propertyConstructorArgument('checkpoint.s3.credentials.secretKey', 'YOUR_S3_SECRET_KEY');

                            break;

                        case 'Properties':
                            credentialsBean = new Bean('com.amazonaws.auth.PropertiesCredentials', 'awsCredentials', {});

                            const fileBean = new Bean('java.io.File', '', spi.S3.awsCredentials.Properties)
                                .pathConstructorArgument('path');

                            if (fileBean.nonEmpty())
                                credentialsBean.beanConstructorArgument('file', fileBean);

                            break;

                        case 'Anonymous':
                            credentialsBean = new Bean('com.amazonaws.auth.AnonymousAWSCredentials', 'awsCredentials', {});

                            break;

                        case 'BasicSession':
                            credentialsBean = new Bean('com.amazonaws.auth.BasicSessionCredentials', 'awsCredentials', {});

                            // TODO 2054 Arguments in one line is very long string.
                            credentialsBean.propertyConstructorArgument('checkpoint.s3.credentials.accessKey')
                                .propertyConstructorArgument('checkpoint.s3.credentials.secretKey')
                                .propertyConstructorArgument('checkpoint.s3.credentials.sessionToken');

                            break;

                        case 'Custom':
                            const className = _.get(spi.S3.awsCredentials, 'Custom.className');

                            if (className)
                                credentialsBean = new Bean(className, 'awsCredentials', {});

                            break;

                        default:
                            break;
                    }

                    if (credentialsBean)
                        s3Bean.beanProperty('awsCredentials', credentialsBean);

                    s3Bean.stringProperty('bucketNameSuffix');

                    if (available('2.4.0')) {
                        s3Bean.stringProperty('bucketEndpoint')
                            .stringProperty('SSEAlgorithm');
                    }

                    const clientBean = new Bean('com.amazonaws.ClientConfiguration', 'clientCfg', spi.S3.clientConfiguration,
                        clusterDflts.checkpointSpi.S3.clientConfiguration);

                    clientBean.enumProperty('protocol')
                        .intProperty('maxConnections')
                        .stringProperty('userAgentPrefix')
                        .stringProperty('userAgentSuffix');

                    const locAddr = new Bean('java.net.InetAddress', '', spi.S3.clientConfiguration)
                        .factoryMethod('getByName')
                        .stringConstructorArgument('localAddress');

                    if (locAddr.nonEmpty())
                        clientBean.beanProperty('localAddress', locAddr);

                    clientBean.stringProperty('proxyHost')
                        .intProperty('proxyPort')
                        .stringProperty('proxyUsername');

                    const userName = clientBean.valueOf('proxyUsername');

                    if (userName)
                        clientBean.property('proxyPassword', `checkpoint.s3.proxy.${userName}.password`);

                    clientBean.stringProperty('proxyDomain')
                        .stringProperty('proxyWorkstation')
                        .stringProperty('nonProxyHosts');

                    const retryPolicy = spi.S3.clientConfiguration.retryPolicy;

                    if (retryPolicy) {
                        const kind = retryPolicy.kind;

                        const policy = retryPolicy[kind];

                        let retryBean;

                        switch (kind) {
                            case 'Default':
                                retryBean = new Bean('com.amazonaws.retry.RetryPolicy', 'retryPolicy', {
                                    retryCondition: 'DEFAULT_RETRY_CONDITION',
                                    backoffStrategy: 'DEFAULT_BACKOFF_STRATEGY',
                                    maxErrorRetry: 'DEFAULT_MAX_ERROR_RETRY',
                                    honorMaxErrorRetryInClientConfig: true
                                }, clusterDflts.checkpointSpi.S3.clientConfiguration.retryPolicy);

                                retryBean.constantConstructorArgument('retryCondition')
                                    .constantConstructorArgument('backoffStrategy')
                                    .constantConstructorArgument('maxErrorRetry')
                                    .constructorArgument('java.lang.Boolean', retryBean.valueOf('honorMaxErrorRetryInClientConfig'));

                                break;

                            case 'DefaultMaxRetries':
                                retryBean = new Bean('com.amazonaws.retry.RetryPolicy', 'retryPolicy', {
                                    retryCondition: 'DEFAULT_RETRY_CONDITION',
                                    backoffStrategy: 'DEFAULT_BACKOFF_STRATEGY',
                                    maxErrorRetry: _.get(policy, 'maxErrorRetry') || -1,
                                    honorMaxErrorRetryInClientConfig: false
                                }, clusterDflts.checkpointSpi.S3.clientConfiguration.retryPolicy);

                                retryBean.constantConstructorArgument('retryCondition')
                                    .constantConstructorArgument('backoffStrategy')
                                    .constructorArgument('java.lang.Integer', retryBean.valueOf('maxErrorRetry'))
                                    .constructorArgument('java.lang.Boolean', retryBean.valueOf('honorMaxErrorRetryInClientConfig'));

                                break;

                            case 'DynamoDB':
                                retryBean = new Bean('com.amazonaws.retry.RetryPolicy', 'retryPolicy', {
                                    retryCondition: 'DEFAULT_RETRY_CONDITION',
                                    backoffStrategy: 'DYNAMODB_DEFAULT_BACKOFF_STRATEGY',
                                    maxErrorRetry: 'DYNAMODB_DEFAULT_MAX_ERROR_RETRY',
                                    honorMaxErrorRetryInClientConfig: true
                                }, clusterDflts.checkpointSpi.S3.clientConfiguration.retryPolicy);

                                retryBean.constantConstructorArgument('retryCondition')
                                    .constantConstructorArgument('backoffStrategy')
                                    .constantConstructorArgument('maxErrorRetry')
                                    .constructorArgument('java.lang.Boolean', retryBean.valueOf('honorMaxErrorRetryInClientConfig'));

                                break;

                            case 'DynamoDBMaxRetries':
                                retryBean = new Bean('com.amazonaws.retry.RetryPolicy', 'retryPolicy', {
                                    retryCondition: 'DEFAULT_RETRY_CONDITION',
                                    backoffStrategy: 'DYNAMODB_DEFAULT_BACKOFF_STRATEGY',
                                    maxErrorRetry: _.get(policy, 'maxErrorRetry') || -1,
                                    honorMaxErrorRetryInClientConfig: false
                                }, clusterDflts.checkpointSpi.S3.clientConfiguration.retryPolicy);

                                retryBean.constantConstructorArgument('retryCondition')
                                    .constantConstructorArgument('backoffStrategy')
                                    .constructorArgument('java.lang.Integer', retryBean.valueOf('maxErrorRetry'))
                                    .constructorArgument('java.lang.Boolean', retryBean.valueOf('honorMaxErrorRetryInClientConfig'));

                                break;

                            case 'Custom':
                                retryBean = new Bean('com.amazonaws.retry.RetryPolicy', 'retryPolicy', policy,
                                    clusterDflts.checkpointSpi.S3.clientConfiguration.retryPolicy);

                                retryBean.beanConstructorArgument('retryCondition', retryBean.valueOf('retryCondition') ? new EmptyBean(retryBean.valueOf('retryCondition')) : null)
                                    .beanConstructorArgument('backoffStrategy', retryBean.valueOf('backoffStrategy') ? new EmptyBean(retryBean.valueOf('backoffStrategy')) : null)
                                    .constructorArgument('java.lang.Integer', retryBean.valueOf('maxErrorRetry'))
                                    .constructorArgument('java.lang.Boolean', retryBean.valueOf('honorMaxErrorRetryInClientConfig'));

                                break;

                            default:
                                break;
                        }

                        if (retryBean)
                            clientBean.beanProperty('retryPolicy', retryBean);
                    }

                    clientBean.intProperty('maxErrorRetry')
                        .intProperty('socketTimeout')
                        .intProperty('connectionTimeout')
                        .intProperty('requestTimeout')
                        .stringProperty('signerOverride')
                        .longProperty('connectionTTL')
                        .longProperty('connectionMaxIdleMillis')
                        .emptyBeanProperty('dnsResolver')
                        .intProperty('responseMetadataCacheSize')
                        .emptyBeanProperty('secureRandom')
                        .intProperty('clientExecutionTimeout')
                        .boolProperty('useReaper')
                        .boolProperty('cacheResponseMetadata')
                        .boolProperty('useExpectContinue')
                        .boolProperty('useThrottleRetries')
                        .boolProperty('useGzip')
                        .boolProperty('preemptiveBasicProxyAuth')
                        .boolProperty('useTcpKeepAlive');

                    if (clientBean.nonEmpty())
                        s3Bean.beanProperty('clientConfiguration', clientBean);

                    s3Bean.emptyBeanProperty('checkpointListener');

                    return s3Bean;

                case 'JDBC':
                    const jdbcBean = new Bean('org.apache.ignite.spi.checkpoint.jdbc.JdbcCheckpointSpi',
                        'checkpointSpiJdbc', spi.JDBC, clusterDflts.checkpointSpi.JDBC);

                    const id = jdbcBean.valueOf('dataSourceBean');
                    const dialect = _.get(spi.JDBC, 'dialect');

                    jdbcBean.dataSource(id, 'dataSource', this.dataSourceBean(id, dialect));

                    if (!_.isEmpty(jdbcBean.valueOf('user'))) {
                        jdbcBean.stringProperty('user')
                            .property('pwd', `checkpoint.${jdbcBean.valueOf('dataSourceBean')}.${jdbcBean.valueOf('user')}.jdbc.password`, 'YOUR_PASSWORD');
                    }

                    jdbcBean.stringProperty('checkpointTableName')
                        .stringProperty('keyFieldName')
                        .stringProperty('keyFieldType')
                        .stringProperty('valueFieldName')
                        .stringProperty('valueFieldType')
                        .stringProperty('expireDateFieldName')
                        .stringProperty('expireDateFieldType')
                        .intProperty('numberOfRetries')
                        .emptyBeanProperty('checkpointListener');

                    return jdbcBean;

                case 'Custom':
                    const clsName = _.get(spi, 'Custom.className');

                    if (clsName)
                        return new Bean(clsName, 'checkpointSpiCustom', spi.Cache);

                    return null;

                default:
                    return null;
            }
        }), (checkpointBean) => nonNil(checkpointBean));

        cfg.arrayProperty('checkpointSpi', 'checkpointSpi', cfgs, 'org.apache.ignite.spi.checkpoint.CheckpointSpi');

        return cfg;
    }

    // Generate cluster query group.
    static clusterClientConnector(cluster, available, cfg = this.igniteConfigurationBean(cluster)) {
        if (!available('2.3.0'))
            return cfg;

        cfg.longProperty('longQueryWarningTimeout');

        if (_.get(cluster, 'clientConnectorConfiguration.enabled') !== true)
            return cfg;

        const bean = new Bean('org.apache.ignite.configuration.ClientConnectorConfiguration', 'cliConnCfg',
            cluster.clientConnectorConfiguration, clusterDflts.clientConnectorConfiguration);

        bean.stringProperty('host')
            .intProperty('port')
            .intProperty('portRange')
            .intProperty('socketSendBufferSize')
            .intProperty('socketReceiveBufferSize')
            .intProperty('maxOpenCursorsPerConnection')
            .intProperty('threadPoolSize')
            .boolProperty('tcpNoDelay');

        if (available('2.4.0')) {
            bean.longProperty('idleTimeout')
                .boolProperty('jdbcEnabled')
                .boolProperty('odbcEnabled')
                .boolProperty('thinClientEnabled');
        }

        if (available('2.5.0')) {
            bean.boolProperty('sslEnabled')
                .boolProperty('sslClientAuth')
                .boolProperty('useIgniteSslContextFactory')
                .emptyBeanProperty('sslContextFactory');
        }

        cfg.beanProperty('clientConnectorConfiguration', bean);

        return cfg;
    }

    // Generate collision group.
    static clusterCollision(collision, cfg = this.igniteConfigurationBean()) {
        let colSpi;

        switch (_.get(collision, 'kind')) {
            case 'JobStealing':
                colSpi = new Bean('org.apache.ignite.spi.collision.jobstealing.JobStealingCollisionSpi',
                    'colSpi', collision.JobStealing, clusterDflts.collision.JobStealing);

                colSpi.intProperty('activeJobsThreshold')
                    .intProperty('waitJobsThreshold')
                    .longProperty('messageExpireTime')
                    .intProperty('maximumStealingAttempts')
                    .boolProperty('stealingEnabled')
                    .emptyBeanProperty('externalCollisionListener')
                    .mapProperty('stealingAttrs', 'stealingAttributes');

                break;
            case 'FifoQueue':
                colSpi = new Bean('org.apache.ignite.spi.collision.fifoqueue.FifoQueueCollisionSpi',
                    'colSpi', collision.FifoQueue, clusterDflts.collision.FifoQueue);

                colSpi.intProperty('parallelJobsNumber')
                    .intProperty('waitingJobsNumber');

                break;
            case 'PriorityQueue':
                colSpi = new Bean('org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi',
                    'colSpi', collision.PriorityQueue, clusterDflts.collision.PriorityQueue);

                colSpi.intProperty('parallelJobsNumber')
                    .intProperty('waitingJobsNumber')
                    .stringProperty('priorityAttributeKey')
                    .stringProperty('jobPriorityAttributeKey')
                    .intProperty('defaultPriority')
                    .intProperty('starvationIncrement')
                    .boolProperty('starvationPreventionEnabled');

                break;
            case 'Custom':
                if (nonNil(_.get(collision, 'Custom.class')))
                    colSpi = new EmptyBean(collision.Custom.class);

                break;
            default:
                return cfg;
        }

        if (nonNil(colSpi))
            cfg.beanProperty('collisionSpi', colSpi);

        return cfg;
    }

    // Generate communication group.
    static clusterCommunication(cluster, available, cfg = this.igniteConfigurationBean(cluster)) {
        const commSpi = new Bean('org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi', 'communicationSpi',
            cluster.communication, clusterDflts.communication);

        commSpi.emptyBeanProperty('listener')
            .stringProperty('localAddress')
            .intProperty('localPort')
            .intProperty('localPortRange')
            .intProperty('sharedMemoryPort')
            .intProperty('directBuffer')
            .intProperty('directSendBuffer')
            .longProperty('idleConnectionTimeout')
            .longProperty('connectTimeout')
            .longProperty('maxConnectTimeout')
            .intProperty('reconnectCount')
            .intProperty('socketSendBuffer')
            .intProperty('socketReceiveBuffer')
            .intProperty('messageQueueLimit')
            .intProperty('slowClientQueueLimit')
            .intProperty('tcpNoDelay')
            .intProperty('ackSendThreshold')
            .intProperty('unacknowledgedMessagesBufferSize')
            .longProperty('socketWriteTimeout')
            .intProperty('selectorsCount')
            .emptyBeanProperty('addressResolver');

        if (commSpi.nonEmpty())
            cfg.beanProperty('communicationSpi', commSpi);

        cfg.longProperty('networkTimeout')
            .longProperty('networkSendRetryDelay')
            .intProperty('networkSendRetryCount');

        if (available(['1.0.0', '2.3.0']))
            cfg.longProperty('discoveryStartupDelay');

        return cfg;
    }

    // Generate REST access configuration.
    static clusterConnector(connector, cfg = this.igniteConfigurationBean()) {
        const connCfg = new Bean('org.apache.ignite.configuration.ConnectorConfiguration',
            'connectorConfiguration', connector, clusterDflts.connector);

        if (connCfg.valueOf('enabled')) {
            connCfg.pathProperty('jettyPath')
                .stringProperty('host')
                .intProperty('port')
                .intProperty('portRange')
                .longProperty('idleTimeout')
                .longProperty('idleQueryCursorTimeout')
                .longProperty('idleQueryCursorCheckFrequency')
                .intProperty('receiveBufferSize')
                .intProperty('sendBufferSize')
                .intProperty('sendQueueLimit')
                .intProperty('directBuffer')
                .intProperty('noDelay')
                .intProperty('selectorCount')
                .intProperty('threadPoolSize')
                .emptyBeanProperty('messageInterceptor')
                .stringProperty('secretKey');

            if (connCfg.valueOf('sslEnabled')) {
                connCfg.intProperty('sslClientAuth')
                    .emptyBeanProperty('sslFactory');
            }

            if (connCfg.nonEmpty())
                cfg.beanProperty('connectorConfiguration', connCfg);
        }

        return cfg;
    }

    // Generate deployment group.
    static clusterDeployment(cluster, available, cfg = this.igniteConfigurationBean(cluster)) {
        cfg.enumProperty('deploymentMode')
            .boolProperty('peerClassLoadingEnabled');

        if (cfg.valueOf('peerClassLoadingEnabled')) {
            cfg.intProperty('peerClassLoadingMissedResourcesCacheSize')
                .intProperty('peerClassLoadingThreadPoolSize')
                .varArgProperty('p2pLocClsPathExcl', 'peerClassLoadingLocalClassPathExclude',
                    cluster.peerClassLoadingLocalClassPathExclude);
        }

        // Since ignite 2.0
        if (available('2.0.0'))
            cfg.emptyBeanProperty('classLoader');

        let deploymentBean = null;

        switch (_.get(cluster, 'deploymentSpi.kind')) {
            case 'URI':
                const uriDeployment = cluster.deploymentSpi.URI;

                deploymentBean = new Bean('org.apache.ignite.spi.deployment.uri.UriDeploymentSpi', 'deploymentSpi', uriDeployment);

                const scanners = _.map(uriDeployment.scanners, (scanner) => new EmptyBean(scanner));

                deploymentBean.collectionProperty('uriList', 'uriList', uriDeployment.uriList)
                    .stringProperty('temporaryDirectoryPath')
                    .varArgProperty('scanners', 'scanners', scanners,
                        'org.apache.ignite.spi.deployment.uri.scanners.UriDeploymentScanner')
                    .emptyBeanProperty('listener')
                    .boolProperty('checkMd5')
                    .boolProperty('encodeUri');

                cfg.beanProperty('deploymentSpi', deploymentBean);

                break;

            case 'Local':
                deploymentBean = new Bean('org.apache.ignite.spi.deployment.local.LocalDeploymentSpi', 'deploymentSpi', cluster.deploymentSpi.Local);

                deploymentBean.emptyBeanProperty('listener');

                cfg.beanProperty('deploymentSpi', deploymentBean);

                break;

            case 'Custom':
                cfg.emptyBeanProperty('deploymentSpi.Custom.className');

                break;

            default:
                // No-op.
        }

        return cfg;
    }

    // Generate discovery group.
    static clusterDiscovery(discovery, available, cfg = this.igniteConfigurationBean(), discoSpi = this.discoveryConfigurationBean(discovery)) {
        discoSpi.stringProperty('localAddress')
            .intProperty('localPort')
            .intProperty('localPortRange')
            .emptyBeanProperty('addressResolver')
            .longProperty('socketTimeout')
            .longProperty('ackTimeout')
            .longProperty('maxAckTimeout')
            .longProperty('networkTimeout')
            .longProperty('joinTimeout')
            .intProperty('threadPriority');

        // Removed in ignite 2.0
        if (available(['1.0.0', '2.0.0'])) {
            discoSpi.intProperty('heartbeatFrequency')
                .intProperty('maxMissedHeartbeats')
                .intProperty('maxMissedClientHeartbeats');
        }

        discoSpi.longProperty('topHistorySize')
            .emptyBeanProperty('listener')
            .emptyBeanProperty('dataExchange')
            .emptyBeanProperty('metricsProvider')
            .intProperty('reconnectCount')
            .longProperty('statisticsPrintFrequency')
            .longProperty('ipFinderCleanFrequency')
            .emptyBeanProperty('authenticator')
            .intProperty('forceServerMode')
            .intProperty('clientReconnectDisabled');

        if (discoSpi.nonEmpty())
            cfg.beanProperty('discoverySpi', discoSpi);

        return discoSpi;
    }

    // Execute event filtration in accordance to generated project version.
    static filterEvents(eventGrps, available) {
        if (available('2.0.0')) {
            return _.reduce(eventGrps, (acc, eventGrp) => {
                switch (eventGrp.value) {
                    case 'EVTS_SWAPSPACE':
                        // Removed.

                        break;
                    case 'EVTS_CACHE':
                        const eventGrpX2 = _.cloneDeep(eventGrp);

                        eventGrpX2.events = _.filter(eventGrpX2.events, (ev) =>
                            !_.includes(['EVT_CACHE_OBJECT_SWAPPED', 'EVT_CACHE_OBJECT_UNSWAPPED'], ev));

                        acc.push(eventGrpX2);

                        break;
                    default:
                        acc.push(eventGrp);
                }

                return acc;
            }, []);
        }

        return eventGrps;
    }

    // Generate events group.
    static clusterEvents(cluster, available, cfg = this.igniteConfigurationBean(cluster)) {
        const eventStorage = cluster.eventStorage;

        let eventStorageBean = null;

        switch (_.get(eventStorage, 'kind')) {
            case 'Memory':
                eventStorageBean = new Bean('org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi', 'eventStorage', eventStorage.Memory, clusterDflts.eventStorage.Memory);

                eventStorageBean.longProperty('expireAgeMs')
                    .longProperty('expireCount')
                    .emptyBeanProperty('filter');

                break;

            case 'Custom':
                const className = _.get(eventStorage, 'Custom.className');

                if (className)
                    eventStorageBean = new EmptyBean(className);

                break;

            default:
                // No-op.
        }

        if (eventStorageBean) {
            if (!eventStorageBean.isEmpty() || !available(['1.0.0', '2.0.0']))
                cfg.beanProperty('eventStorageSpi', eventStorageBean);

            if (nonEmpty(cluster.includeEventTypes)) {
                const eventGrps = _.filter(this.eventGrps, ({value}) => _.includes(cluster.includeEventTypes, value));

                cfg.eventTypes('evts', 'includeEventTypes', this.filterEvents(eventGrps, available));
            }
        }

        return cfg;
    }

    // Generate failover group.
    static clusterFailover(cluster, available, cfg = this.igniteConfigurationBean(cluster)) {
        const spis = [];

        // Since ignite 2.0
        if (available('2.0.0')) {
            cfg.longProperty('failureDetectionTimeout')
                .longProperty('clientFailureDetectionTimeout');
        }

        _.forEach(cluster.failoverSpi, (spi) => {
            let failoverSpi;

            switch (_.get(spi, 'kind')) {
                case 'JobStealing':
                    failoverSpi = new Bean('org.apache.ignite.spi.failover.jobstealing.JobStealingFailoverSpi',
                        'failoverSpi', spi.JobStealing, clusterDflts.failoverSpi.JobStealing);

                    failoverSpi.intProperty('maximumFailoverAttempts');

                    break;
                case 'Never':
                    failoverSpi = new Bean('org.apache.ignite.spi.failover.never.NeverFailoverSpi',
                        'failoverSpi', spi.Never);

                    break;
                case 'Always':
                    failoverSpi = new Bean('org.apache.ignite.spi.failover.always.AlwaysFailoverSpi',
                        'failoverSpi', spi.Always, clusterDflts.failoverSpi.Always);

                    failoverSpi.intProperty('maximumFailoverAttempts');

                    break;
                case 'Custom':
                    const className = _.get(spi, 'Custom.class');

                    if (className)
                        failoverSpi = new EmptyBean(className);

                    break;
                default:
                    // No-op.
            }

            if (failoverSpi)
                spis.push(failoverSpi);
        });

        if (spis.length)
            cfg.arrayProperty('failoverSpi', 'failoverSpi', spis, 'org.apache.ignite.spi.failover.FailoverSpi');

        return cfg;
    }

    // Generate failover group.
    static clusterHadoop(hadoop, cfg = this.igniteConfigurationBean()) {
        const hadoopBean = new Bean('org.apache.ignite.configuration.HadoopConfiguration', 'hadoop', hadoop, clusterDflts.hadoopConfiguration);

        let plannerBean;

        switch (_.get(hadoop, 'mapReducePlanner.kind')) {
            case 'Weighted':
                plannerBean = new Bean('org.apache.ignite.hadoop.mapreduce.IgniteHadoopWeightedMapReducePlanner', 'planner',
                    _.get(hadoop, 'mapReducePlanner.Weighted'), clusterDflts.hadoopConfiguration.mapReducePlanner.Weighted);

                plannerBean.intProperty('localMapperWeight')
                    .intProperty('remoteMapperWeight')
                    .intProperty('localReducerWeight')
                    .intProperty('remoteReducerWeight')
                    .intProperty('preferLocalReducerThresholdWeight');

                break;

            case 'Custom':
                const clsName = _.get(hadoop, 'mapReducePlanner.Custom.className');

                if (clsName)
                    plannerBean = new EmptyBean(clsName);

                break;

            default:
                // No-op.
        }

        if (plannerBean)
            hadoopBean.beanProperty('mapReducePlanner', plannerBean);

        hadoopBean.longProperty('finishedJobInfoTtl')
            .intProperty('maxParallelTasks')
            .intProperty('maxTaskQueueSize')
            .arrayProperty('nativeLibraryNames', 'nativeLibraryNames', _.get(hadoop, 'nativeLibraryNames'));

        if (!hadoopBean.isEmpty())
            cfg.beanProperty('hadoopConfiguration', hadoopBean);

        return cfg;
    }

    // Generate load balancing configuration group.
    static clusterLoadBalancing(cluster, cfg = this.igniteConfigurationBean(cluster)) {
        const spis = [];

        _.forEach(cluster.loadBalancingSpi, (spi) => {
            let loadBalancingSpi;

            switch (_.get(spi, 'kind')) {
                case 'RoundRobin':
                    loadBalancingSpi = new Bean('org.apache.ignite.spi.loadbalancing.roundrobin.RoundRobinLoadBalancingSpi', 'loadBalancingSpiRR', spi.RoundRobin, clusterDflts.loadBalancingSpi.RoundRobin);

                    loadBalancingSpi.boolProperty('perTask');

                    break;
                case 'Adaptive':
                    loadBalancingSpi = new Bean('org.apache.ignite.spi.loadbalancing.adaptive.AdaptiveLoadBalancingSpi', 'loadBalancingSpiAdaptive', spi.Adaptive);

                    let probeBean;

                    switch (_.get(spi, 'Adaptive.loadProbe.kind')) {
                        case 'Job':
                            probeBean = new Bean('org.apache.ignite.spi.loadbalancing.adaptive.AdaptiveJobCountLoadProbe', 'jobProbe', spi.Adaptive.loadProbe.Job, clusterDflts.loadBalancingSpi.Adaptive.loadProbe.Job);

                            probeBean.boolProperty('useAverage');

                            break;
                        case 'CPU':
                            probeBean = new Bean('org.apache.ignite.spi.loadbalancing.adaptive.AdaptiveCpuLoadProbe', 'cpuProbe', spi.Adaptive.loadProbe.CPU, clusterDflts.loadBalancingSpi.Adaptive.loadProbe.CPU);

                            probeBean.boolProperty('useAverage')
                                .boolProperty('useProcessors')
                                .intProperty('processorCoefficient');

                            break;
                        case 'ProcessingTime':
                            probeBean = new Bean('org.apache.ignite.spi.loadbalancing.adaptive.AdaptiveProcessingTimeLoadProbe', 'timeProbe', spi.Adaptive.loadProbe.ProcessingTime, clusterDflts.loadBalancingSpi.Adaptive.loadProbe.ProcessingTime);

                            probeBean.boolProperty('useAverage');

                            break;
                        case 'Custom':
                            const className = _.get(spi, 'Adaptive.loadProbe.Custom.className');

                            if (className)
                                probeBean = new Bean(className, 'probe', spi.Adaptive.loadProbe.Job.Custom);

                            break;
                        default:
                            // No-op.
                    }

                    if (probeBean)
                        loadBalancingSpi.beanProperty('loadProbe', probeBean);

                    break;
                case 'WeightedRandom':
                    loadBalancingSpi = new Bean('org.apache.ignite.spi.loadbalancing.weightedrandom.WeightedRandomLoadBalancingSpi', 'loadBalancingSpiRandom', spi.WeightedRandom, clusterDflts.loadBalancingSpi.WeightedRandom);

                    loadBalancingSpi.intProperty('nodeWeight')
                        .boolProperty('useWeights');

                    break;
                case 'Custom':
                    const className = _.get(spi, 'Custom.className');

                    if (className)
                        loadBalancingSpi = new Bean(className, 'loadBalancingSpiCustom', spi.Custom);

                    break;
                default:
                    // No-op.
            }

            if (loadBalancingSpi)
                spis.push(loadBalancingSpi);
        });

        if (spis.length)
            cfg.varArgProperty('loadBalancingSpi', 'loadBalancingSpi', spis, 'org.apache.ignite.spi.loadbalancing.LoadBalancingSpi');

        return cfg;
    }

    // Generate logger group.
    static clusterLogger(logger, cfg = this.igniteConfigurationBean()) {
        let loggerBean;

        switch (_.get(logger, 'kind')) {
            case 'Log4j':
                if (logger.Log4j && (logger.Log4j.mode === 'Default' || logger.Log4j.mode === 'Path' && nonEmpty(logger.Log4j.path))) {
                    loggerBean = new Bean('org.apache.ignite.logger.log4j.Log4JLogger',
                        'logger', logger.Log4j, clusterDflts.logger.Log4j);

                    if (loggerBean.valueOf('mode') === 'Path')
                        loggerBean.pathConstructorArgument('path');

                    loggerBean.enumProperty('level');
                }

                break;
            case 'Log4j2':
                if (logger.Log4j2 && nonEmpty(logger.Log4j2.path)) {
                    loggerBean = new Bean('org.apache.ignite.logger.log4j2.Log4J2Logger',
                        'logger', logger.Log4j2, clusterDflts.logger.Log4j2);

                    loggerBean.pathConstructorArgument('path')
                        .enumProperty('level');
                }

                break;
            case 'Null':
                loggerBean = new EmptyBean('org.apache.ignite.logger.NullLogger');

                break;
            case 'Java':
                loggerBean = new EmptyBean('org.apache.ignite.logger.java.JavaLogger');

                break;
            case 'JCL':
                loggerBean = new EmptyBean('org.apache.ignite.logger.jcl.JclLogger');

                break;
            case 'SLF4J':
                loggerBean = new EmptyBean('org.apache.ignite.logger.slf4j.Slf4jLogger');

                break;
            case 'Custom':
                if (logger.Custom && nonEmpty(logger.Custom.class))
                    loggerBean = new EmptyBean(logger.Custom.class);

                break;
            default:
                return cfg;
        }

        if (loggerBean)
            cfg.beanProperty('gridLogger', loggerBean);

        return cfg;
    }

    // Generate memory configuration.
    static clusterMemory(memoryConfiguration, available, cfg = this.igniteConfigurationBean()) {
        const memoryBean = new Bean('org.apache.ignite.configuration.MemoryConfiguration', 'memoryConfiguration', memoryConfiguration, clusterDflts.memoryConfiguration);

        memoryBean.intProperty('pageSize')
            .intProperty('concurrencyLevel')
            .longProperty('systemCacheInitialSize')
            .longProperty('systemCacheMaxSize')
            .stringProperty('defaultMemoryPolicyName');

        if (memoryBean.valueOf('defaultMemoryPolicyName') === 'default')
            memoryBean.longProperty('defaultMemoryPolicySize');

        const policies = [];

        _.forEach(_.get(memoryConfiguration, 'memoryPolicies'), (plc) => {
            const plcBean = new Bean('org.apache.ignite.configuration.MemoryPolicyConfiguration', 'policy', plc, clusterDflts.memoryConfiguration.memoryPolicies);

            plcBean.stringProperty('name')
                .longProperty('initialSize')
                .longProperty('maxSize')
                .stringProperty('swapFilePath')
                .enumProperty('pageEvictionMode')
                .doubleProperty('evictionThreshold')
                .intProperty('emptyPagesPoolSize')
                .boolProperty('metricsEnabled');

            if (available('2.1.0')) {
                plcBean.intProperty('subIntervals')
                    .longProperty('rateTimeInterval');
            }

            if (plcBean.isEmpty())
                return;

            policies.push(plcBean);
        });

        if (!_.isEmpty(policies))
            memoryBean.varArgProperty('memoryPolicies', 'memoryPolicies', policies, 'org.apache.ignite.configuration.MemoryPolicyConfiguration');

        if (memoryBean.isEmpty())
            return cfg;

        cfg.beanProperty('memoryConfiguration', memoryBean);

        return cfg;
    }

    static dataRegionConfiguration(dataRegionCfg) {
        const plcBean = new Bean('org.apache.ignite.configuration.DataRegionConfiguration', 'dataRegionCfg', dataRegionCfg, clusterDflts.dataStorageConfiguration.dataRegionConfigurations);

        plcBean.stringProperty('name')
            .longProperty('initialSize')
            .longProperty('maxSize')
            .stringProperty('swapPath')
            .enumProperty('pageEvictionMode')
            .doubleProperty('evictionThreshold')
            .intProperty('emptyPagesPoolSize')
            .intProperty('metricsSubIntervalCount')
            .longProperty('metricsRateTimeInterval')
            .longProperty('checkpointPageBufferSize')
            .boolProperty('metricsEnabled');

        if (!plcBean.valueOf('swapPath'))
            plcBean.boolProperty('persistenceEnabled');

        return plcBean;
    }

    // Generate data storage configuration.
    static clusterDataStorageConfiguration(dataStorageCfg, available, cfg = this.igniteConfigurationBean()) {
        if (!available('2.3.0'))
            return cfg;

        const storageBean = new Bean('org.apache.ignite.configuration.DataStorageConfiguration', 'dataStorageCfg', dataStorageCfg, clusterDflts.dataStorageConfiguration);

        storageBean.intProperty('pageSize')
            .intProperty('concurrencyLevel')
            .longProperty('systemRegionInitialSize')
            .longProperty('systemRegionMaxSize');

        const dfltDataRegionCfg = this.dataRegionConfiguration(_.get(dataStorageCfg, 'defaultDataRegionConfiguration'));

        if (!dfltDataRegionCfg.isEmpty())
            storageBean.beanProperty('defaultDataRegionConfiguration', dfltDataRegionCfg);

        const dataRegionCfgs = [];

        _.forEach(_.get(dataStorageCfg, 'dataRegionConfigurations'), (dataRegionCfg) => {
            const plcBean = this.dataRegionConfiguration(dataRegionCfg);

            if (plcBean.isEmpty())
                return;

            dataRegionCfgs.push(plcBean);
        });

        if (!_.isEmpty(dataRegionCfgs))
            storageBean.varArgProperty('dataRegionConfigurations', 'dataRegionConfigurations', dataRegionCfgs, 'org.apache.ignite.configuration.DataRegionConfiguration');

        storageBean.stringProperty('storagePath')
            .longProperty('checkpointFrequency')
            .intProperty('checkpointThreads')
            .enumProperty('checkpointWriteOrder')
            .enumProperty('walMode')
            .stringProperty('walPath')
            .stringProperty('walArchivePath')
            .intProperty('walSegments')
            .intProperty('walSegmentSize')
            .intProperty('walHistorySize');

        if (available('2.4.0'))
            storageBean.intProperty('walBufferSize');

        storageBean.longProperty('walFlushFrequency')
            .longProperty('walFsyncDelayNanos')
            .intProperty('walRecordIteratorBufferSize')
            .longProperty('lockWaitTime')
            .intProperty('walThreadLocalBufferSize')
            .intProperty('metricsSubIntervalCount')
            .longProperty('metricsRateTimeInterval')
            .longProperty('walAutoArchiveAfterInactivity')
            .boolProperty('metricsEnabled')
            .boolProperty('alwaysWriteFullPages')
            .boolProperty('writeThrottlingEnabled');

        if (available('2.4.0'))
            storageBean.boolProperty('walCompactionEnabled');

        const fileIOFactory = _.get(dataStorageCfg, 'fileIOFactory');

        let factoryBean;

        if (fileIOFactory === 'RANDOM')
            factoryBean = new Bean('org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory', 'rndFileIoFactory', {});
        else if (fileIOFactory === 'ASYNC')
            factoryBean = new Bean('org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory', 'asyncFileIoFactory', {});

        if (factoryBean)
            storageBean.beanProperty('fileIOFactory', factoryBean);

        if (storageBean.isEmpty())
            return cfg;

        cfg.beanProperty('dataStorageConfiguration', storageBean);

        return cfg;
    }

    // Generate miscellaneous configuration.
    static clusterMisc(cluster, available, cfg = this.igniteConfigurationBean(cluster)) {
        cfg.stringProperty('workDirectory');

        // Since Ignite 2.0
        if (available('2.0.0')) {
            cfg.stringProperty('consistentId')
                .emptyBeanProperty('warmupClosure')
                .boolProperty('activeOnStart')
                .boolProperty('cacheSanityCheckEnabled');
        }

        if (available(['1.0.0', '2.1.0']))
            cfg.boolProperty('lateAffinityAssignment');

        return cfg;
    }

    // Generate IGFSs configs.
    static clusterIgfss(igfss, available, cfg = this.igniteConfigurationBean()) {
        const igfsCfgs = _.map(igfss, (igfs) => {
            const igfsCfg = this.igfsGeneral(igfs, available);

            this.igfsIPC(igfs, igfsCfg);
            this.igfsFragmentizer(igfs, igfsCfg);

            // Removed in ignite 2.0
            if (available(['1.0.0', '2.0.0']))
                this.igfsDualMode(igfs, igfsCfg);

            this.igfsSecondFS(igfs, igfsCfg);
            this.igfsMisc(igfs, available, igfsCfg);

            return igfsCfg;
        });

        cfg.varArgProperty('igfsCfgs', 'fileSystemConfiguration', igfsCfgs, 'org.apache.ignite.configuration.FileSystemConfiguration');

        return cfg;
    }

    // Generate marshaller group.
    static clusterMarshaller(cluster, available, cfg = this.igniteConfigurationBean(cluster)) {
        if (available(['1.0.0', '2.1.0'])) {
            const kind = _.get(cluster.marshaller, 'kind');
            const settings = _.get(cluster.marshaller, kind);

            let bean;

            switch (kind) {
                case 'OptimizedMarshaller':
                    if (available(['1.0.0', '2.0.0'])) {
                        bean = new Bean('org.apache.ignite.marshaller.optimized.OptimizedMarshaller', 'marshaller', settings)
                            .intProperty('poolSize')
                            .intProperty('requireSerializable');
                    }

                    break;

                case 'JdkMarshaller':
                    bean = new Bean('org.apache.ignite.marshaller.jdk.JdkMarshaller', 'marshaller', settings);

                    break;

                default:
                // No-op.
            }

            if (bean)
                cfg.beanProperty('marshaller', bean);
        }

        cfg.intProperty('marshalLocalJobs');

        // Removed in ignite 2.0
        if (available(['1.0.0', '2.0.0'])) {
            cfg.intProperty('marshallerCacheKeepAliveTime')
                .intProperty('marshallerCacheThreadPoolSize', 'marshallerCachePoolSize');
        }

        return cfg;
    }

    // Generate metrics group.
    static clusterMetrics(cluster, available, cfg = this.igniteConfigurationBean(cluster)) {
        cfg.longProperty('metricsExpireTime')
            .intProperty('metricsHistorySize')
            .longProperty('metricsLogFrequency');

        // Since ignite 2.0
        if (available('2.0.0'))
            cfg.longProperty('metricsUpdateFrequency');

        return cfg;
    }

    // Generate ODBC group.
    static clusterODBC(odbc, available, cfg = this.igniteConfigurationBean()) {
        //  Deprecated in ignite 2.1
        if (available('2.1.0') || _.get(odbc, 'odbcEnabled') !== true)
            return cfg;

        const bean = new Bean('org.apache.ignite.configuration.OdbcConfiguration', 'odbcConfiguration',
            odbc, clusterDflts.odbcConfiguration);

        bean.stringProperty('endpointAddress')
            .intProperty('socketSendBufferSize')
            .intProperty('socketReceiveBufferSize')
            .intProperty('maxOpenCursors')
            .intProperty('threadPoolSize');

        cfg.beanProperty('odbcConfiguration', bean);

        return cfg;
    }

    // Generate cluster query group.
    static clusterQuery(cluster, available, cfg = this.igniteConfigurationBean(cluster)) {
        if (!available('2.1.0'))
            return cfg;

        cfg.longProperty('longQueryWarningTimeout');

        if (_.get(cluster, 'sqlConnectorConfiguration.enabled') !== true)
            return cfg;

        const bean = new Bean('org.apache.ignite.configuration.SqlConnectorConfiguration', 'sqlConnCfg',
            cluster.sqlConnectorConfiguration, clusterDflts.sqlConnectorConfiguration);

        bean.stringProperty('host')
            .intProperty('port')
            .intProperty('portRange')
            .intProperty('socketSendBufferSize')
            .intProperty('socketReceiveBufferSize')
            .intProperty('maxOpenCursorsPerConnection')
            .intProperty('threadPoolSize')
            .boolProperty('tcpNoDelay');

        cfg.beanProperty('sqlConnectorConfiguration', bean);

        return cfg;
    }

    // Generate cluster query group.
    static clusterPersistence(persistence, available, cfg = this.igniteConfigurationBean()) {
        if (!available(['2.1.0', '2.3.0']) || _.get(persistence, 'enabled') !== true)
            return cfg;

        const bean = new Bean('org.apache.ignite.configuration.PersistentStoreConfiguration', 'PersistenceCfg',
            persistence, clusterDflts.persistenceStoreConfiguration);

        bean.stringProperty('persistentStorePath')
            .boolProperty('metricsEnabled')
            .boolProperty('alwaysWriteFullPages')
            .longProperty('checkpointingFrequency')
            .longProperty('checkpointingPageBufferSize')
            .intProperty('checkpointingThreads')
            .stringProperty('walStorePath')
            .stringProperty('walArchivePath')
            .intProperty('walSegments')
            .intProperty('walSegmentSize')
            .intProperty('walHistorySize')
            .longProperty('walFlushFrequency')
            .longProperty('walFsyncDelayNanos')
            .intProperty('walRecordIteratorBufferSize')
            .longProperty('lockWaitTime')
            .longProperty('rateTimeInterval')
            .intProperty('tlbSize')
            .intProperty('subIntervals');

        cfg.beanProperty('persistentStoreConfiguration', bean);

        return cfg;
    }

    // Java code generator for cluster's service configurations.
    static clusterServiceConfiguration(srvs, caches, cfg = this.igniteConfigurationBean()) {
        const srvBeans = [];

        _.forEach(srvs, (srv) => {
            const bean = new Bean('org.apache.ignite.services.ServiceConfiguration', 'service', srv, clusterDflts.serviceConfigurations);

            bean.stringProperty('name')
                .emptyBeanProperty('service')
                .intProperty('maxPerNodeCount')
                .intProperty('totalCount')
                .stringProperty('cache', 'cacheName', (_id) => _id ? _.get(_.find(caches, {_id}), 'name', null) : null)
                .stringProperty('affinityKey');

            srvBeans.push(bean);
        });

        if (!_.isEmpty(srvBeans))
            cfg.arrayProperty('services', 'serviceConfiguration', srvBeans, 'org.apache.ignite.services.ServiceConfiguration');

        return cfg;
    }

    // Java code generator for cluster's SSL configuration.
    static clusterSsl(cluster, cfg = this.igniteConfigurationBean(cluster)) {
        if (cluster.sslEnabled && nonNil(cluster.sslContextFactory)) {
            const bean = new Bean('org.apache.ignite.ssl.SslContextFactory', 'sslCtxFactory',
                cluster.sslContextFactory);

            bean.intProperty('keyAlgorithm')
                .pathProperty('keyStoreFilePath');

            if (nonEmpty(bean.valueOf('keyStoreFilePath')))
                bean.propertyChar('keyStorePassword', 'ssl.key.storage.password', 'YOUR_SSL_KEY_STORAGE_PASSWORD');

            bean.intProperty('keyStoreType')
                .intProperty('protocol');

            if (nonEmpty(cluster.sslContextFactory.trustManagers)) {
                bean.arrayProperty('trustManagers', 'trustManagers',
                    _.map(cluster.sslContextFactory.trustManagers, (clsName) => new EmptyBean(clsName)),
                    'javax.net.ssl.TrustManager');
            }
            else {
                bean.pathProperty('trustStoreFilePath');

                if (nonEmpty(bean.valueOf('trustStoreFilePath')))
                    bean.propertyChar('trustStorePassword', 'ssl.trust.storage.password', 'YOUR_SSL_TRUST_STORAGE_PASSWORD');

                bean.intProperty('trustStoreType');
            }

            cfg.beanProperty('sslContextFactory', bean);
        }

        return cfg;
    }

    // Generate swap group.
    static clusterSwap(cluster, cfg = this.igniteConfigurationBean(cluster)) {
        if (_.get(cluster.swapSpaceSpi, 'kind') === 'FileSwapSpaceSpi') {
            const bean = new Bean('org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi', 'swapSpaceSpi',
                cluster.swapSpaceSpi.FileSwapSpaceSpi);

            bean.pathProperty('baseDirectory')
                .intProperty('readStripesNumber')
                .floatProperty('maximumSparsity')
                .intProperty('maxWriteQueueSize')
                .intProperty('writeBufferSize');

            cfg.beanProperty('swapSpaceSpi', bean);
        }

        return cfg;
    }

    // Generate time group.
    static clusterTime(cluster, available, cfg = this.igniteConfigurationBean(cluster)) {
        if (available(['1.0.0', '2.0.0'])) {
            cfg.intProperty('clockSyncSamples')
                .intProperty('clockSyncFrequency');
        }

        cfg.intProperty('timeServerPortBase')
            .intProperty('timeServerPortRange');

        return cfg;
    }

    // Generate thread pools group.
    static clusterPools(cluster, available, cfg = this.igniteConfigurationBean(cluster)) {
        cfg.intProperty('publicThreadPoolSize')
            .intProperty('systemThreadPoolSize')
            .intProperty('serviceThreadPoolSize')
            .intProperty('managementThreadPoolSize')
            .intProperty('igfsThreadPoolSize')
            .intProperty('rebalanceThreadPoolSize')
            .intProperty('utilityCacheThreadPoolSize', 'utilityCachePoolSize')
            .longProperty('utilityCacheKeepAliveTime')
            .intProperty('asyncCallbackPoolSize')
            .intProperty('stripedPoolSize');

        // Since ignite 2.0
        if (available('2.0.0')) {
            cfg.intProperty('dataStreamerThreadPoolSize')
                .intProperty('queryThreadPoolSize');

            const executors = [];

            _.forEach(cluster.executorConfiguration, (exec) => {
                const execBean = new Bean('org.apache.ignite.configuration.ExecutorConfiguration', 'executor', exec);

                execBean.stringProperty('name')
                    .intProperty('size');

                if (!execBean.isEmpty())
                    executors.push(execBean);
            });

            if (!_.isEmpty(executors))
                cfg.arrayProperty('executors', 'executorConfiguration', executors, 'org.apache.ignite.configuration.ExecutorConfiguration');
        }

        return cfg;
    }

    // Generate transactions group.
    static clusterTransactions(transactionConfiguration, cfg = this.igniteConfigurationBean()) {
        const bean = new Bean('org.apache.ignite.configuration.TransactionConfiguration', 'transactionConfiguration',
            transactionConfiguration, clusterDflts.transactionConfiguration);

        bean.enumProperty('defaultTxConcurrency')
            .enumProperty('defaultTxIsolation')
            .longProperty('defaultTxTimeout')
            .intProperty('pessimisticTxLogLinger')
            .intProperty('pessimisticTxLogSize')
            .boolProperty('txSerializableEnabled')
            .emptyBeanProperty('txManagerFactory');

        if (bean.nonEmpty())
            cfg.beanProperty('transactionConfiguration', bean);

        return cfg;
    }

    // Generate user attributes group.
    static clusterUserAttributes(cluster, cfg = this.igniteConfigurationBean(cluster)) {
        cfg.mapProperty('attrs', 'attributes', 'userAttributes');

        return cfg;
    }

    // Generate domain model for general group.
    static domainModelGeneral(domain, cfg = this.domainConfigurationBean(domain)) {
        switch (cfg.valueOf('queryMetadata')) {
            case 'Annotations':
                if (nonNil(domain.keyType) && nonNil(domain.valueType)) {
                    cfg.varArgProperty('indexedTypes', 'indexedTypes',
                        [javaTypes.fullClassName(domain.keyType), javaTypes.fullClassName(domain.valueType)],
                        'java.lang.Class');
                }

                break;
            case 'Configuration':
                cfg.stringProperty('keyType', 'keyType', (val) => javaTypes.fullClassName(val))
                    .stringProperty('valueType', 'valueType', (val) => javaTypes.fullClassName(val));

                break;
            default:
        }

        return cfg;
    }

    // Generate domain model for query group.
    static domainModelQuery(domain, available, cfg = this.domainConfigurationBean(domain)) {
        if (cfg.valueOf('queryMetadata') === 'Configuration') {
            const fields = _.filter(_.map(domain.fields,
                (e) => ({name: e.name, className: javaTypes.stringClassName(e.className)})), (field) => {
                return field.name !== domain.keyFieldName && field.name !== domain.valueFieldName;
            });

            cfg.stringProperty('tableName');

            if (available('2.0.0')) {
                cfg.stringProperty('keyFieldName')
                    .stringProperty('valueFieldName');

                const keyFieldName = cfg.valueOf('keyFieldName');
                const valFieldName = cfg.valueOf('valueFieldName');

                if (keyFieldName)
                    fields.push({name: keyFieldName, className: javaTypes.stringClassName(domain.keyType)});

                if (valFieldName)
                    fields.push({name: valFieldName, className: javaTypes.stringClassName(domain.valueType)});
            }

            cfg.collectionProperty('keyFields', 'keyFields', domain.queryKeyFields, 'java.lang.String', 'java.util.HashSet')
                .mapProperty('fields', fields, 'fields', true)
                .mapProperty('aliases', 'aliases');

            const indexes = _.map(domain.indexes, (index) =>
                new Bean('org.apache.ignite.cache.QueryIndex', 'index', index, cacheDflts.indexes)
                    .stringProperty('name')
                    .enumProperty('indexType')
                    .mapProperty('indFlds', 'fields', 'fields', true)
            );

            cfg.collectionProperty('indexes', 'indexes', indexes, 'org.apache.ignite.cache.QueryIndex');
        }

        return cfg;
    }

    // Generate domain model db fields.
    static _domainModelDatabaseFields(cfg, propName, domain) {
        const fields = _.map(domain[propName], (field) => {
            return new Bean('org.apache.ignite.cache.store.jdbc.JdbcTypeField', 'typeField', field, cacheDflts.typeField)
                .constantConstructorArgument('databaseFieldType')
                .stringConstructorArgument('databaseFieldName')
                .classConstructorArgument('javaFieldType')
                .stringConstructorArgument('javaFieldName');
        });

        cfg.varArgProperty(propName, propName, fields, 'org.apache.ignite.cache.store.jdbc.JdbcTypeField');

        return cfg;
    }

    // Generate domain model for store group.
    static domainStore(domain, cfg = this.domainJdbcTypeBean(domain)) {
        cfg.stringProperty('databaseSchema')
            .stringProperty('databaseTable');

        this._domainModelDatabaseFields(cfg, 'keyFields', domain);
        this._domainModelDatabaseFields(cfg, 'valueFields', domain);

        return cfg;
    }

    /**
     * Generate eviction policy object.
     * @param {Object} ccfg Parent configuration.
     * @param {Function} available Function to check feature is supported in Ignite current version.
     * @param {Boolean} near Near cache flag.
     * @param {Object} src Source.
     * @param {Object} dflt Default.
     * @returns {Object} Parent configuration.
     * @private
     */
    static _evictionPolicy(ccfg, available, near, src, dflt) {
        let propName;
        let beanProps;

        if (available('2.4.0')) {
            switch (_.get(src, 'kind')) {
                case 'LRU': beanProps = {cls: 'org.apache.ignite.cache.eviction.lru.LruEvictionPolicyFactory', src: src.LRU };
                    break;

                case 'FIFO': beanProps = {cls: 'org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicyFactory', src: src.FIFO };
                    break;

                case 'SORTED': beanProps = {cls: 'org.apache.ignite.cache.eviction.sorted.SortedEvictionPolicyFactory', src: src.SORTED };
                    break;

                default:
                    return ccfg;
            }

            propName = (near ? 'nearEviction' : 'eviction') + 'PolicyFactory';
        }
        else {
            switch (_.get(src, 'kind')) {
                case 'LRU': beanProps = {cls: 'org.apache.ignite.cache.eviction.lru.LruEvictionPolicy', src: src.LRU };
                    break;

                case 'FIFO': beanProps = {cls: 'org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy', src: src.FIFO };
                    break;

                case 'SORTED': beanProps = {cls: 'org.apache.ignite.cache.eviction.sorted.SortedEvictionPolicy', src: src.SORTED };
                    break;

                default:
                    return ccfg;
            }

            propName = (near ? 'nearEviction' : 'eviction') + 'Policy';
        }

        const bean = new Bean(beanProps.cls, propName, beanProps.src, dflt);

        bean.intProperty('batchSize')
            .intProperty('maxMemorySize')
            .intProperty('maxSize');

        ccfg.beanProperty(propName, bean);

        return ccfg;
    }

    // Generate cache general group.
    static cacheGeneral(cache, available, ccfg = this.cacheConfigurationBean(cache)) {
        ccfg.stringProperty('name');

        if (available('2.1.0'))
            ccfg.stringProperty('groupName');

        ccfg.enumProperty('cacheMode')
            .enumProperty('atomicityMode');

        if (ccfg.valueOf('cacheMode') === 'PARTITIONED' && ccfg.valueOf('backups')) {
            ccfg.intProperty('backups')
                .intProperty('readFromBackup');
        }

        // Since ignite 2.0
        if (available('2.0.0'))
            ccfg.enumProperty('partitionLossPolicy');

        ccfg.intProperty('copyOnRead');

        if (ccfg.valueOf('cacheMode') === 'PARTITIONED' && ccfg.valueOf('atomicityMode') === 'TRANSACTIONAL')
            ccfg.intProperty('isInvalidate', 'invalidate');

        return ccfg;
    }

    // Generation of constructor for affinity function.
    static affinityFunction(cls, func) {
        const affBean = new Bean(cls, 'affinityFunction', func);

        affBean.boolConstructorArgument('excludeNeighbors')
            .intProperty('partitions')
            .emptyBeanProperty('affinityBackupFilter');

        return affBean;
    }

    // Generate affinity function.
    static affinity(affinity, cfg) {
        switch (_.get(affinity, 'kind')) {
            case 'Rendezvous':
                cfg.beanProperty('affinity', this.affinityFunction('org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction', affinity.Rendezvous));

                break;
            case 'Fair':
                cfg.beanProperty('affinity', this.affinityFunction('org.apache.ignite.cache.affinity.fair.FairAffinityFunction', affinity.Fair));

                break;
            case 'Custom':
                cfg.emptyBeanProperty('affinity.Custom.className', 'affinity');

                break;
            default:
            // No-op.
        }
    }

    // Generate cache memory group.
    static cacheAffinity(cache, available, ccfg = this.cacheConfigurationBean(cache)) {
        this.affinity(cache.affinity, ccfg);

        ccfg.emptyBeanProperty('affinityMapper');

        // Since ignite 2.0
        if (available('2.0.0'))
            ccfg.emptyBeanProperty('topologyValidator');

        return ccfg;
    }

    // Generate cache memory group.
    static cacheMemory(cache, available, ccfg = this.cacheConfigurationBean(cache)) {
        // Since ignite 2.0
        if (available(['2.0.0', '2.3.0']))
            ccfg.stringProperty('memoryPolicyName');

        if (available('2.3.0'))
            ccfg.stringProperty('dataRegionName');

        // Removed in ignite 2.0
        if (available(['1.0.0', '2.0.0'])) {
            ccfg.enumProperty('memoryMode');

            if (ccfg.valueOf('memoryMode') !== 'OFFHEAP_VALUES')
                ccfg.longProperty('offHeapMaxMemory');
        }

        // Since ignite 2.0
        if (available('2.0.0')) {
            ccfg.boolProperty('onheapCacheEnabled')
                .emptyBeanProperty('evictionFilter');
        }

        this._evictionPolicy(ccfg, available, false, cache.evictionPolicy, cacheDflts.evictionPolicy);

        // Removed in ignite 2.0
        if (available(['1.0.0', '2.0.0'])) {
            ccfg.intProperty('startSize')
                .boolProperty('swapEnabled');
        }

        return ccfg;
    }

    // Generate cache queries & Indexing group.
    static cacheQuery(cache, domains, available, ccfg = this.cacheConfigurationBean(cache)) {
        const indexedTypes = _.reduce(domains, (acc, domain) => {
            if (domain.queryMetadata === 'Annotations')
                acc.push(javaTypes.fullClassName(domain.keyType), javaTypes.fullClassName(domain.valueType));

            return acc;
        }, []);

        ccfg.stringProperty('sqlSchema');

        // Removed in ignite 2.0
        if (available(['1.0.0', '2.0.0']))
            ccfg.intProperty('sqlOnheapRowCacheSize');

        ccfg.longProperty('longQueryWarningTimeout')
            .arrayProperty('indexedTypes', 'indexedTypes', indexedTypes, 'java.lang.Class')
            .intProperty('queryDetailMetricsSize')
            .arrayProperty('sqlFunctionClasses', 'sqlFunctionClasses', cache.sqlFunctionClasses, 'java.lang.Class');

        // Removed in ignite 2.0
        if (available(['1.0.0', '2.0.0']))
            ccfg.intProperty('snapshotableIndex');

        ccfg.intProperty('sqlEscapeAll');

        // Since ignite 2.0
        if (available('2.0.0')) {
            ccfg.intProperty('queryParallelism')
                .intProperty('sqlIndexMaxInlineSize');
        }

        return ccfg;
    }

    // Generate cache store group.
    static cacheStore(cache, domains, available, ccfg = this.cacheConfigurationBean(cache)) {
        const kind = _.get(cache, 'cacheStoreFactory.kind');

        if (kind && cache.cacheStoreFactory[kind]) {
            let bean = null;

            const storeFactory = cache.cacheStoreFactory[kind];

            switch (kind) {
                case 'CacheJdbcPojoStoreFactory':
                    bean = new Bean('org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory', 'cacheStoreFactory',
                        storeFactory, cacheDflts.cacheStoreFactory.CacheJdbcPojoStoreFactory);

                    const jdbcId = bean.valueOf('dataSourceBean');

                    bean.dataSource(jdbcId, 'dataSourceBean', this.dataSourceBean(jdbcId, storeFactory.dialect))
                        .beanProperty('dialect', new EmptyBean(this.dialectClsName(storeFactory.dialect)));

                    bean.intProperty('batchSize')
                        .intProperty('maximumPoolSize')
                        .intProperty('maximumWriteAttempts')
                        .intProperty('parallelLoadCacheMinimumThreshold')
                        .emptyBeanProperty('hasher')
                        .emptyBeanProperty('transformer')
                        .boolProperty('sqlEscapeAll');

                    const setType = (typeBean, propName) => {
                        if (javaTypes.nonBuiltInClass(typeBean.valueOf(propName)))
                            typeBean.stringProperty(propName);
                        else
                            typeBean.classProperty(propName);
                    };

                    const types = _.reduce(domains, (acc, domain) => {
                        if (isNil(domain.databaseTable))
                            return acc;

                        const typeBean = this.domainJdbcTypeBean(_.merge({}, domain, {cacheName: cache.name}))
                            .stringProperty('cacheName');

                        setType(typeBean, 'keyType');
                        setType(typeBean, 'valueType');

                        this.domainStore(domain, typeBean);

                        acc.push(typeBean);

                        return acc;
                    }, []);

                    bean.varArgProperty('types', 'types', types, 'org.apache.ignite.cache.store.jdbc.JdbcType');

                    break;
                case 'CacheJdbcBlobStoreFactory':
                    bean = new Bean('org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStoreFactory', 'cacheStoreFactory',
                        storeFactory);

                    if (bean.valueOf('connectVia') === 'DataSource') {
                        const blobId = bean.valueOf('dataSourceBean');

                        bean.dataSource(blobId, 'dataSourceBean', this.dataSourceBean(blobId, storeFactory.dialect));
                    }
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
                .longProperty('writeBehindFlushFrequency')
                .intProperty('writeBehindFlushThreadCount');

            // Since ignite 2.0
            if (available('2.0.0'))
                ccfg.boolProperty('writeBehindCoalescing');
        }

        return ccfg;
    }

    // Generate cache concurrency control group.
    static cacheConcurrency(cache, available, ccfg = this.cacheConfigurationBean(cache)) {
        ccfg.intProperty('maxConcurrentAsyncOperations')
            .longProperty('defaultLockTimeout');

        // Removed in ignite 2.0
        if (available(['1.0.0', '2.0.0']))
            ccfg.enumProperty('atomicWriteOrderMode');

        ccfg.enumProperty('writeSynchronizationMode');

        return ccfg;
    }

    static nodeFilter(filter, igfss) {
        const kind = _.get(filter, 'kind');

        const settings = _.get(filter, kind);

        if (!isNil(settings)) {
            switch (kind) {
                case 'IGFS':
                    const foundIgfs = _.find(igfss, {_id: settings.igfs});

                    if (foundIgfs) {
                        return new Bean('org.apache.ignite.internal.processors.igfs.IgfsNodePredicate', 'nodeFilter', foundIgfs)
                            .stringConstructorArgument('name');
                    }

                    break;
                case 'Custom':
                    if (nonEmpty(settings.className))
                        return new EmptyBean(settings.className);

                    break;
                default:
                // No-op.
            }
        }

        return null;
    }

    // Generate cache node filter group.
    static cacheNodeFilter(cache, igfss, ccfg = this.cacheConfigurationBean(cache)) {
        const filter = _.get(cache, 'nodeFilter');

        const filterBean = this.nodeFilter(filter, igfss);

        if (filterBean)
            ccfg.beanProperty('nodeFilter', filterBean);

        return ccfg;
    }

    // Generate cache rebalance group.
    static cacheRebalance(cache, ccfg = this.cacheConfigurationBean(cache)) {
        if (ccfg.valueOf('cacheMode') !== 'LOCAL') {
            ccfg.enumProperty('rebalanceMode')
                .intProperty('rebalanceThreadPoolSize')
                .intProperty('rebalanceBatchSize')
                .longProperty('rebalanceBatchesPrefetchCount')
                .intProperty('rebalanceOrder')
                .longProperty('rebalanceDelay')
                .longProperty('rebalanceTimeout')
                .longProperty('rebalanceThrottle');
        }

        if (ccfg.includes('igfsAffinnityGroupSize')) {
            const bean = new Bean('org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper', 'affinityMapper', cache)
                .intConstructorArgument('igfsAffinnityGroupSize');

            ccfg.beanProperty('affinityMapper', bean);
        }

        return ccfg;
    }

    // Generate server near cache group.
    static cacheNearServer(cache, available, ccfg = this.cacheConfigurationBean(cache)) {
        if (ccfg.valueOf('cacheMode') === 'PARTITIONED' && _.get(cache, 'nearConfiguration.enabled')) {
            const bean = new Bean('org.apache.ignite.configuration.NearCacheConfiguration', 'nearConfiguration',
                cache.nearConfiguration, cacheDflts.nearConfiguration);

            bean.intProperty('nearStartSize');

            this._evictionPolicy(bean, available, true, bean.valueOf('nearEvictionPolicy'), cacheDflts.evictionPolicy);

            ccfg.beanProperty('nearConfiguration', bean);
        }

        return ccfg;
    }

    // Generate client near cache group.
    static cacheNearClient(cache, available, ccfg = this.cacheConfigurationBean(cache)) {
        if (ccfg.valueOf('cacheMode') === 'PARTITIONED' && _.get(cache, 'clientNearConfiguration.enabled')) {
            const bean = new Bean('org.apache.ignite.configuration.NearCacheConfiguration',
                javaTypes.toJavaName('nearConfiguration', ccfg.valueOf('name')),
                cache.clientNearConfiguration, cacheDflts.clientNearConfiguration);

            bean.intProperty('nearStartSize');

            this._evictionPolicy(bean, available, true, bean.valueOf('nearEvictionPolicy'), cacheDflts.evictionPolicy);

            return bean;
        }

        return ccfg;
    }

    // Generate cache statistics group.
    static cacheStatistics(cache, ccfg = this.cacheConfigurationBean(cache)) {
        ccfg.boolProperty('statisticsEnabled')
            .boolProperty('managementEnabled');

        return ccfg;
    }

    // Generate domain models configs.
    static cacheDomains(domains, available, ccfg) {
        const qryEntities = _.reduce(domains, (acc, domain) => {
            if (isNil(domain.queryMetadata) || domain.queryMetadata === 'Configuration') {
                const qryEntity = this.domainModelGeneral(domain);

                this.domainModelQuery(domain, available, qryEntity);

                acc.push(qryEntity);
            }

            return acc;
        }, []);

        ccfg.collectionProperty('qryEntities', 'queryEntities', qryEntities, 'org.apache.ignite.cache.QueryEntity');
    }

    static cacheConfiguration(cache, available, ccfg = this.cacheConfigurationBean(cache)) {
        this.cacheGeneral(cache, available, ccfg);
        this.cacheAffinity(cache, available, ccfg);
        this.cacheMemory(cache, available, ccfg);
        this.cacheQuery(cache, cache.domains, available, ccfg);
        this.cacheStore(cache, cache.domains, available, ccfg);

        const igfs = _.get(cache, 'nodeFilter.IGFS.instance');
        this.cacheNodeFilter(cache, igfs ? [igfs] : [], ccfg);
        this.cacheConcurrency(cache, available, ccfg);
        this.cacheRebalance(cache, ccfg);
        this.cacheNearServer(cache, available, ccfg);
        this.cacheStatistics(cache, ccfg);
        this.cacheDomains(cache.domains, available, ccfg);

        return ccfg;
    }

    // Generate IGFS general group.
    static igfsGeneral(igfs, available, cfg = this.igfsConfigurationBean(igfs)) {
        if (_.isEmpty(igfs.name))
            return cfg;

        cfg.stringProperty('name');

        // Removed in ignite 2.0
        if (available(['1.0.0', '2.0.0'])) {
            cfg.stringProperty('name', 'dataCacheName', (name) => name + '-data')
                .stringProperty('name', 'metaCacheName', (name) => name + '-meta');
        }

        cfg.enumProperty('defaultMode');

        return cfg;
    }

    // Generate IGFS secondary file system group.
    static igfsSecondFS(igfs, cfg = this.igfsConfigurationBean(igfs)) {
        if (igfs.secondaryFileSystemEnabled) {
            const secondFs = igfs.secondaryFileSystem || {};

            const bean = new Bean('org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem',
                'secondaryFileSystem', secondFs, igfsDflts.secondaryFileSystem);

            bean.stringProperty('userName', 'defaultUserName');

            const factoryBean = new Bean('org.apache.ignite.hadoop.fs.CachingHadoopFileSystemFactory',
                'fac', secondFs);

            factoryBean.stringProperty('uri')
                .pathProperty('cfgPath', 'configPaths');

            bean.beanProperty('fileSystemFactory', factoryBean);

            cfg.beanProperty('secondaryFileSystem', bean);
        }

        return cfg;
    }

    // Generate IGFS IPC group.
    static igfsIPC(igfs, cfg = this.igfsConfigurationBean(igfs)) {
        if (igfs.ipcEndpointEnabled) {
            const bean = new Bean('org.apache.ignite.igfs.IgfsIpcEndpointConfiguration', 'ipcEndpointConfiguration',
                igfs.ipcEndpointConfiguration, igfsDflts.ipcEndpointConfiguration);

            bean.enumProperty('type')
                .stringProperty('host')
                .intProperty('port')
                .intProperty('memorySize')
                .pathProperty('tokenDirectoryPath')
                .intProperty('threadCount');

            if (bean.nonEmpty())
                cfg.beanProperty('ipcEndpointConfiguration', bean);
        }

        return cfg;
    }

    // Generate IGFS fragmentizer group.
    static igfsFragmentizer(igfs, cfg = this.igfsConfigurationBean(igfs)) {
        if (igfs.fragmentizerEnabled) {
            cfg.intProperty('fragmentizerConcurrentFiles')
                .longProperty('fragmentizerThrottlingBlockLength')
                .longProperty('fragmentizerThrottlingDelay');
        }
        else
            cfg.boolProperty('fragmentizerEnabled');

        return cfg;
    }

    // Generate IGFS Dual mode group.
    static igfsDualMode(igfs, cfg = this.igfsConfigurationBean(igfs)) {
        cfg.intProperty('dualModeMaxPendingPutsSize')
            .emptyBeanProperty('dualModePutExecutorService')
            .intProperty('dualModePutExecutorServiceShutdown');

        return cfg;
    }

    // Generate IGFS miscellaneous group.
    static igfsMisc(igfs, available, cfg = this.igfsConfigurationBean(igfs)) {
        cfg.intProperty('blockSize');

        // Removed in ignite 2.0
        if (available(['1.0.0', '2.0.0']))
            cfg.intProperty('streamBufferSize');

        // Since ignite 2.0
        if (available('2.0.0'))
            cfg.intProperty('streamBufferSize', 'bufferSize');

        // Removed in ignite 2.0
        if (available(['1.0.0', '2.0.0']))
            cfg.intProperty('maxSpaceSize');

        cfg.longProperty('maximumTaskRangeLength')
            .intProperty('managementPort')
            .intProperty('perNodeBatchSize')
            .intProperty('perNodeParallelBatchCount')
            .intProperty('prefetchBlocks')
            .intProperty('sequentialReadsBeforePrefetch');

        // Removed in ignite 2.0
        if (available(['1.0.0', '2.0.0']))
            cfg.intProperty('trashPurgeTimeout');

        cfg.intProperty('colocateMetadata')
            .intProperty('relaxedConsistency')
            .mapProperty('pathModes', 'pathModes');

        // Since ignite 2.0
        if (available('2.0.0'))
            cfg.boolProperty('updateFileLengthOnFlush');

        return cfg;
    }
}
