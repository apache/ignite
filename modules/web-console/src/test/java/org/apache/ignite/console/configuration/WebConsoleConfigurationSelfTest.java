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

package org.apache.ignite.console.configuration;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.eviction.sorted.SortedEvictionPolicy;
import org.apache.ignite.cache.store.hibernate.CacheHibernateBlobStore;
import org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStore;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.cache.store.jdbc.JdbcType;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.ExecutorConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.OdbcConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.SqlConnectorConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.hadoop.fs.CachingHadoopFileSystemFactory;
import org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem;
import org.apache.ignite.hadoop.fs.KerberosHadoopFileSystemFactory;
import org.apache.ignite.hadoop.mapreduce.IgniteHadoopWeightedMapReducePlanner;
import org.apache.ignite.hadoop.util.BasicUserNameMapper;
import org.apache.ignite.hadoop.util.ChainedUserNameMapper;
import org.apache.ignite.hadoop.util.KerberosUserNameMapper;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.checkpoint.cache.CacheCheckpointSpi;
import org.apache.ignite.spi.checkpoint.jdbc.JdbcCheckpointSpi;
import org.apache.ignite.spi.checkpoint.s3.S3CheckpointSpi;
import org.apache.ignite.spi.checkpoint.sharedfs.SharedFsCheckpointSpi;
import org.apache.ignite.spi.collision.fifoqueue.FifoQueueCollisionSpi;
import org.apache.ignite.spi.collision.jobstealing.JobStealingCollisionSpi;
import org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.deployment.local.LocalDeploymentSpi;
import org.apache.ignite.spi.deployment.uri.UriDeploymentSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
import org.apache.ignite.spi.failover.always.AlwaysFailoverSpi;
import org.apache.ignite.spi.failover.jobstealing.JobStealingFailoverSpi;
import org.apache.ignite.spi.loadbalancing.adaptive.AdaptiveCpuLoadProbe;
import org.apache.ignite.spi.loadbalancing.adaptive.AdaptiveJobCountLoadProbe;
import org.apache.ignite.spi.loadbalancing.adaptive.AdaptiveLoadBalancingSpi;
import org.apache.ignite.spi.loadbalancing.adaptive.AdaptiveProcessingTimeLoadProbe;
import org.apache.ignite.spi.loadbalancing.roundrobin.RoundRobinLoadBalancingSpi;
import org.apache.ignite.spi.loadbalancing.weightedrandom.WeightedRandomLoadBalancingSpi;
import org.apache.ignite.ssl.SslContextFactory;
import org.junit.Test;

/**
 * Check difference of Ignite configuration with Ignite Web Console "Configuration" screen.
 */
public class WebConsoleConfigurationSelfTest {
    /** */
    protected static final Set<String> EMPTY_FIELDS = Collections.emptySet();

    /** */
    protected static final Set<String> SPI_EXCLUDED_FIELDS = Collections.singleton("name");

    /** Map of properties metadata by class. */
    protected final Map<Class<?>, MetadataInfo> metadata = new HashMap<>();

    /**
     * @param msg Message to log.
     */
    protected void log(String msg) {
        System.out.println(msg);
    }

    /**
     * Prepare metadata for properties, which are possible to configure.
     */
    @SuppressWarnings("deprecation")
    protected void prepareMetadata() {
        // Cluster configuration.
        Set<String> igniteCfgProps = new HashSet<>();
        igniteCfgProps.add("cacheConfiguration");
        igniteCfgProps.add("discoverySpi");
        igniteCfgProps.add("localHost");
        igniteCfgProps.add("atomicConfiguration");
        igniteCfgProps.add("userAttributes");
        igniteCfgProps.add("binaryConfiguration");
        igniteCfgProps.add("cacheKeyConfiguration");
        igniteCfgProps.add("checkpointSpi");
        igniteCfgProps.add("collisionSpi");
        igniteCfgProps.add("communicationSpi");
        igniteCfgProps.add("networkTimeout");
        igniteCfgProps.add("networkSendRetryDelay");
        igniteCfgProps.add("networkSendRetryCount");
        igniteCfgProps.add("connectorConfiguration");
        igniteCfgProps.add("dataStorageConfiguration");
        igniteCfgProps.add("deploymentMode");
        igniteCfgProps.add("peerClassLoadingEnabled");
        igniteCfgProps.add("peerClassLoadingMissedResourcesCacheSize");
        igniteCfgProps.add("peerClassLoadingThreadPoolSize");
        igniteCfgProps.add("peerClassLoadingLocalClassPathExclude");
        igniteCfgProps.add("classLoader");
        igniteCfgProps.add("deploymentSpi");
        igniteCfgProps.add("eventStorageSpi");
        igniteCfgProps.add("includeEventTypes");
        igniteCfgProps.add("failureDetectionTimeout");
        igniteCfgProps.add("clientFailureDetectionTimeout");
        igniteCfgProps.add("failoverSpi");
        igniteCfgProps.add("hadoopConfiguration");
        igniteCfgProps.add("loadBalancingSpi");
        igniteCfgProps.add("marshalLocalJobs");

        // Removed since 2.0.
        // igniteCfgProps.add("marshallerCacheKeepAliveTime");
        // igniteCfgProps.add("marshallerCacheThreadPoolSize");

        igniteCfgProps.add("metricsExpireTime");
        igniteCfgProps.add("metricsHistorySize");
        igniteCfgProps.add("metricsLogFrequency");
        igniteCfgProps.add("metricsUpdateFrequency");
        igniteCfgProps.add("workDirectory");
        igniteCfgProps.add("consistentId");
        igniteCfgProps.add("warmupClosure");
        igniteCfgProps.add("activeOnStart");
        igniteCfgProps.add("cacheSanityCheckEnabled");
        igniteCfgProps.add("longQueryWarningTimeout");
        igniteCfgProps.add("odbcConfiguration");
        igniteCfgProps.add("serviceConfiguration");
        igniteCfgProps.add("sqlConnectorConfiguration");
        igniteCfgProps.add("sslContextFactory");

        // Removed since 2.0.
        // igniteCfgProps.add("swapSpaceSpi");

        igniteCfgProps.add("publicThreadPoolSize");
        igniteCfgProps.add("systemThreadPoolSize");
        igniteCfgProps.add("serviceThreadPoolSize");
        igniteCfgProps.add("managementThreadPoolSize");
        igniteCfgProps.add("igfsThreadPoolSize");
        igniteCfgProps.add("utilityCacheThreadPoolSize");
        igniteCfgProps.add("utilityCacheKeepAliveTime");
        igniteCfgProps.add("asyncCallbackPoolSize");
        igniteCfgProps.add("stripedPoolSize");
        igniteCfgProps.add("dataStreamerThreadPoolSize");
        igniteCfgProps.add("queryThreadPoolSize");
        igniteCfgProps.add("executorConfiguration");

        // Removed since 2.0.
        // igniteCfgProps.add("clockSyncSamples");
        // igniteCfgProps.add("clockSyncFrequency");

        igniteCfgProps.add("timeServerPortBase");
        igniteCfgProps.add("timeServerPortRange");
        igniteCfgProps.add("transactionConfiguration");
        igniteCfgProps.add("clientConnectorConfiguration");
        igniteCfgProps.add("fileSystemConfiguration");
        igniteCfgProps.add("gridLogger");
        igniteCfgProps.add("pluginConfigurations");
        igniteCfgProps.add("mvccVacuumFrequency");
        igniteCfgProps.add("mvccVacuumThreadCount");
        igniteCfgProps.add("encryptionSpi");
        igniteCfgProps.add("authenticationEnabled");
        igniteCfgProps.add("sqlQueryHistorySize");
        igniteCfgProps.add("lifecycleBeans");
        igniteCfgProps.add("addressResolver");
        igniteCfgProps.add("mBeanServer");
        igniteCfgProps.add("networkCompressionLevel");
        igniteCfgProps.add("systemWorkerBlockedTimeout");
        igniteCfgProps.add("includeProperties");
        igniteCfgProps.add("cacheStoreSessionListenerFactories");
        igniteCfgProps.add("sqlSchemas");
        igniteCfgProps.add("igniteInstanceName");
        igniteCfgProps.add("communicationFailureResolver");
        igniteCfgProps.add("failureHandler");
        igniteCfgProps.add("rebalanceThreadPoolSize");
        igniteCfgProps.add("localEventListeners");

        Set<String> igniteCfgPropsDep = new HashSet<>();
        igniteCfgPropsDep.add("gridName");
        igniteCfgPropsDep.add("lateAffinityAssignment");
        igniteCfgPropsDep.add("persistentStoreConfiguration");
        igniteCfgPropsDep.add("memoryConfiguration");
        igniteCfgPropsDep.add("marshaller");
        igniteCfgPropsDep.add("discoveryStartupDelay");

        Set<String> igniteCfgPropsExcl = new HashSet<>();
        // igniteCfgPropsExcl.add("lifecycleBeans");
        igniteCfgPropsExcl.add("daemon");
        igniteCfgPropsExcl.add("clientMode");
        igniteCfgPropsExcl.add("indexingSpi");
        igniteCfgPropsExcl.add("nodeId");
        igniteCfgPropsExcl.add("platformConfiguration");
        igniteCfgPropsExcl.add("segmentCheckFrequency");
        igniteCfgPropsExcl.add("allSegmentationResolversPassRequired");
        igniteCfgPropsExcl.add("segmentationPolicy");
        igniteCfgPropsExcl.add("segmentationResolveAttempts");
        igniteCfgPropsExcl.add("waitForSegmentOnStart");
        igniteCfgPropsExcl.add("segmentationResolvers");
        igniteCfgPropsExcl.add("autoActivationEnabled");
        igniteCfgPropsExcl.add("igniteHome");
        igniteCfgPropsExcl.add("platformConfiguration");

        metadata.put(IgniteConfiguration.class,
            new MetadataInfo(igniteCfgProps, igniteCfgPropsDep, igniteCfgPropsExcl));

        Set<String> encriptionSpiProps = new HashSet<>();
        encriptionSpiProps.add("keySize");
        encriptionSpiProps.add("masterKeyName");
        encriptionSpiProps.add("keyStorePath");
        metadata.put(KeystoreEncryptionSpi.class, new MetadataInfo(encriptionSpiProps, EMPTY_FIELDS, SPI_EXCLUDED_FIELDS));

        Set<String> cacheKeyCfgProps = new HashSet<>();
        cacheKeyCfgProps.add("typeName");
        cacheKeyCfgProps.add("affinityKeyFieldName");

        metadata.put(CacheKeyConfiguration.class, new MetadataInfo(cacheKeyCfgProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> atomicCfgProps = new HashSet<>();
        atomicCfgProps.add("cacheMode");
        atomicCfgProps.add("atomicSequenceReserveSize");
        atomicCfgProps.add("backups");
        atomicCfgProps.add("affinity");
        atomicCfgProps.add("groupName");

        metadata.put(AtomicConfiguration.class, new MetadataInfo(atomicCfgProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> binaryCfgProps = new HashSet<>();
        binaryCfgProps.add("idMapper");
        binaryCfgProps.add("nameMapper");
        binaryCfgProps.add("serializer");
        binaryCfgProps.add("typeConfigurations");
        binaryCfgProps.add("compactFooter");
        metadata.put(BinaryConfiguration.class, new MetadataInfo(binaryCfgProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> binaryTypeCfgProps = new HashSet<>();
        binaryTypeCfgProps.add("typeName");
        binaryTypeCfgProps.add("idMapper");
        binaryTypeCfgProps.add("nameMapper");
        binaryTypeCfgProps.add("serializer");
        binaryTypeCfgProps.add("enum");
        binaryTypeCfgProps.add("enumValues");
        metadata.put(BinaryTypeConfiguration.class, new MetadataInfo(binaryTypeCfgProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> sharedFsCheckpointProps = new HashSet<>();
        sharedFsCheckpointProps.add("directoryPaths");
        metadata.put(SharedFsCheckpointSpi.class,
            new MetadataInfo(sharedFsCheckpointProps, EMPTY_FIELDS, SPI_EXCLUDED_FIELDS));

        Set<String> s3CheckpointProps = new HashSet<>();
        s3CheckpointProps.add("bucketNameSuffix");
        s3CheckpointProps.add("bucketEndpoint");
        s3CheckpointProps.add("sSEAlgorithm");
        s3CheckpointProps.add("checkpointListener");
        metadata.put(S3CheckpointSpi.class, new MetadataInfo(s3CheckpointProps, EMPTY_FIELDS, SPI_EXCLUDED_FIELDS));

        Set<String> cacheCheckpointProps = new HashSet<>();
        cacheCheckpointProps.add("cacheName");
        metadata.put(CacheCheckpointSpi.class, new MetadataInfo(cacheCheckpointProps, EMPTY_FIELDS, SPI_EXCLUDED_FIELDS));

        Set<String> jdbcCheckpointProps = new HashSet<>();
        // Only setter for dataSource.
        // jdbcCheckpointProps.add("dataSourceBean");
        // jdbcCheckpointProps.add("dialect");
        jdbcCheckpointProps.add("checkpointListener");
        jdbcCheckpointProps.add("user");
        // Only on code generation.
        jdbcCheckpointProps.add("pwd");
        jdbcCheckpointProps.add("checkpointTableName");
        jdbcCheckpointProps.add("numberOfRetries");
        jdbcCheckpointProps.add("keyFieldName");
        jdbcCheckpointProps.add("keyFieldType");
        jdbcCheckpointProps.add("valueFieldName");
        jdbcCheckpointProps.add("valueFieldType");
        jdbcCheckpointProps.add("expireDateFieldName");
        jdbcCheckpointProps.add("expireDateFieldType");
        metadata.put(JdbcCheckpointSpi.class, new MetadataInfo(jdbcCheckpointProps, EMPTY_FIELDS, SPI_EXCLUDED_FIELDS));

        Set<String> cliConProps = new HashSet<>();
        cliConProps.add("host");
        cliConProps.add("port");
        cliConProps.add("portRange");
        cliConProps.add("socketSendBufferSize");
        cliConProps.add("socketReceiveBufferSize");
        cliConProps.add("maxOpenCursorsPerConnection");
        cliConProps.add("threadPoolSize");
        cliConProps.add("tcpNoDelay");
        cliConProps.add("idleTimeout");
        cliConProps.add("sslEnabled");
        cliConProps.add("sslClientAuth");
        cliConProps.add("useIgniteSslContextFactory");
        cliConProps.add("sslContextFactory");
        cliConProps.add("jdbcEnabled");
        cliConProps.add("odbcEnabled");
        cliConProps.add("thinClientEnabled");
        metadata.put(ClientConnectorConfiguration.class, new MetadataInfo(cliConProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> jobStealingCollisionProps = new HashSet<>();
        jobStealingCollisionProps.add("activeJobsThreshold");
        jobStealingCollisionProps.add("waitJobsThreshold");
        jobStealingCollisionProps.add("messageExpireTime");
        jobStealingCollisionProps.add("maximumStealingAttempts");
        jobStealingCollisionProps.add("stealingEnabled");
        jobStealingCollisionProps.add("externalCollisionListener");
        jobStealingCollisionProps.add("stealingAttributes");
        metadata.put(JobStealingCollisionSpi.class,
            new MetadataInfo(jobStealingCollisionProps, EMPTY_FIELDS, SPI_EXCLUDED_FIELDS));

        Set<String> priQueueCollisionProps = new HashSet<>();
        priQueueCollisionProps.add("parallelJobsNumber");
        priQueueCollisionProps.add("waitingJobsNumber");
        priQueueCollisionProps.add("priorityAttributeKey");
        priQueueCollisionProps.add("jobPriorityAttributeKey");
        priQueueCollisionProps.add("defaultPriority");
        priQueueCollisionProps.add("starvationIncrement");
        priQueueCollisionProps.add("starvationPreventionEnabled");
        metadata.put(PriorityQueueCollisionSpi.class, new MetadataInfo(priQueueCollisionProps, EMPTY_FIELDS,
            SPI_EXCLUDED_FIELDS));

        Set<String> fifoQueueCollisionProps = new HashSet<>();
        fifoQueueCollisionProps.add("parallelJobsNumber");
        fifoQueueCollisionProps.add("waitingJobsNumber");
        metadata.put(FifoQueueCollisionSpi.class,
            new MetadataInfo(fifoQueueCollisionProps, EMPTY_FIELDS, SPI_EXCLUDED_FIELDS));

        Set<String> commProps = new HashSet<>();
        commProps.add("listener");
        commProps.add("localAddress");
        commProps.add("localPort");
        commProps.add("localPortRange");
        commProps.add("sharedMemoryPort");
        commProps.add("idleConnectionTimeout");
        commProps.add("connectTimeout");
        commProps.add("maxConnectTimeout");
        commProps.add("reconnectCount");
        commProps.add("socketSendBuffer");
        commProps.add("socketReceiveBuffer");
        commProps.add("slowClientQueueLimit");
        commProps.add("ackSendThreshold");
        commProps.add("messageQueueLimit");
        commProps.add("unacknowledgedMessagesBufferSize");
        commProps.add("socketWriteTimeout");
        commProps.add("selectorsCount");
        commProps.add("addressResolver");
        commProps.add("directBuffer");
        commProps.add("directSendBuffer");
        commProps.add("tcpNoDelay");
        commProps.add("selectorSpins");
        commProps.add("connectionsPerNode");
        commProps.add("usePairedConnections");
        commProps.add("filterReachableAddresses");

        metadata.put(TcpCommunicationSpi.class, new MetadataInfo(commProps, EMPTY_FIELDS, SPI_EXCLUDED_FIELDS));

        Set<String> discoverySpiProps = new HashSet<>();
        discoverySpiProps.add("ipFinder");
        discoverySpiProps.add("localAddress");
        discoverySpiProps.add("localPort");
        discoverySpiProps.add("localPortRange");
        discoverySpiProps.add("addressResolver");
        discoverySpiProps.add("socketTimeout");
        discoverySpiProps.add("ackTimeout");
        discoverySpiProps.add("maxAckTimeout");
        discoverySpiProps.add("networkTimeout");
        discoverySpiProps.add("joinTimeout");
        discoverySpiProps.add("threadPriority");
        // Removed since 2.0.
        // discoverySpiProps.add("heartbeatFrequency");
        // discoverySpiProps.add("maxMissedHeartbeats");
        // discoverySpiProps.add("maxMissedClientHeartbeats");
        discoverySpiProps.add("topHistorySize");
        discoverySpiProps.add("listener");
        discoverySpiProps.add("dataExchange");
        discoverySpiProps.add("metricsProvider");
        discoverySpiProps.add("reconnectCount");
        discoverySpiProps.add("statisticsPrintFrequency");
        discoverySpiProps.add("ipFinderCleanFrequency");
        discoverySpiProps.add("authenticator");
        discoverySpiProps.add("forceServerMode");
        discoverySpiProps.add("clientReconnectDisabled");
        discoverySpiProps.add("connectionRecoveryTimeout");
        discoverySpiProps.add("reconnectDelay");
        discoverySpiProps.add("soLinger");

        Set<String> discoverySpiExclProps = new HashSet<>();
        discoverySpiExclProps.addAll(SPI_EXCLUDED_FIELDS);
        discoverySpiExclProps.add("nodeAttributes");
        metadata.put(TcpDiscoverySpi.class, new MetadataInfo(discoverySpiProps, EMPTY_FIELDS, discoverySpiExclProps));

        Set<String> connectorProps = new HashSet<>();
        connectorProps.add("jettyPath");
        connectorProps.add("host");
        connectorProps.add("port");
        connectorProps.add("portRange");
        connectorProps.add("idleQueryCursorTimeout");
        connectorProps.add("idleQueryCursorCheckFrequency");
        connectorProps.add("idleTimeout");
        connectorProps.add("receiveBufferSize");
        connectorProps.add("sendBufferSize");
        connectorProps.add("sendQueueLimit");
        connectorProps.add("directBuffer");
        connectorProps.add("noDelay");
        connectorProps.add("selectorCount");
        connectorProps.add("threadPoolSize");
        connectorProps.add("messageInterceptor");
        connectorProps.add("secretKey");
        connectorProps.add("sslEnabled");
        connectorProps.add("sslClientAuth");
        connectorProps.add("sslFactory");

        Set<String> connectorPropsDep = new HashSet<>();
        connectorPropsDep.add("sslContextFactory");
        metadata.put(ConnectorConfiguration.class, new MetadataInfo(connectorProps, connectorPropsDep, EMPTY_FIELDS));

        Set<String> dataStorageProps = new HashSet<>();
        dataStorageProps.add("pageSize");
        dataStorageProps.add("concurrencyLevel");
        dataStorageProps.add("systemRegionInitialSize");
        dataStorageProps.add("systemRegionMaxSize");
        dataStorageProps.add("defaultDataRegionConfiguration");
        dataStorageProps.add("dataRegionConfigurations");
        dataStorageProps.add("storagePath");
        dataStorageProps.add("checkpointFrequency");
        dataStorageProps.add("checkpointThreads");
        dataStorageProps.add("checkpointWriteOrder");
        dataStorageProps.add("walMode");
        dataStorageProps.add("walPath");
        dataStorageProps.add("walArchivePath");
        dataStorageProps.add("walSegments");
        dataStorageProps.add("walSegmentSize");
        dataStorageProps.add("walHistorySize");
        dataStorageProps.add("walBufferSize");
        dataStorageProps.add("walFlushFrequency");
        dataStorageProps.add("walFsyncDelayNanos");
        dataStorageProps.add("walRecordIteratorBufferSize");
        dataStorageProps.add("lockWaitTime");
        dataStorageProps.add("walThreadLocalBufferSize");
        dataStorageProps.add("metricsSubIntervalCount");
        dataStorageProps.add("metricsRateTimeInterval");
        dataStorageProps.add("fileIOFactory");
        dataStorageProps.add("walAutoArchiveAfterInactivity");
        dataStorageProps.add("metricsEnabled");
        dataStorageProps.add("alwaysWriteFullPages");
        dataStorageProps.add("writeThrottlingEnabled");
        dataStorageProps.add("checkpointReadLockTimeout");
        dataStorageProps.add("maxWalArchiveSize");
        dataStorageProps.add("walCompactionEnabled");
        dataStorageProps.add("walCompactionLevel");
        metadata.put(DataStorageConfiguration.class, new MetadataInfo(dataStorageProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> dataRegionProps = new HashSet<>();
        dataRegionProps.add("name");
        dataRegionProps.add("initialSize");
        dataRegionProps.add("maxSize");
        dataRegionProps.add("swapPath");
        dataRegionProps.add("checkpointPageBufferSize");
        dataRegionProps.add("pageEvictionMode");
        dataRegionProps.add("evictionThreshold");
        dataRegionProps.add("emptyPagesPoolSize");
        dataRegionProps.add("metricsSubIntervalCount");
        dataRegionProps.add("metricsRateTimeInterval");
        dataRegionProps.add("metricsEnabled");
        dataRegionProps.add("persistenceEnabled");
        metadata.put(DataRegionConfiguration.class, new MetadataInfo(dataRegionProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> uriDeploymentProps = new HashSet<>();
        uriDeploymentProps.add("uriList");
        uriDeploymentProps.add("temporaryDirectoryPath");
        uriDeploymentProps.add("scanners");
        uriDeploymentProps.add("listener");
        uriDeploymentProps.add("checkMd5");
        uriDeploymentProps.add("encodeUri");
        metadata.put(UriDeploymentSpi.class, new MetadataInfo(uriDeploymentProps, EMPTY_FIELDS, SPI_EXCLUDED_FIELDS));

        Set<String> locDeploymentProps = new HashSet<>();
        locDeploymentProps.add("listener");
        metadata.put(LocalDeploymentSpi.class, new MetadataInfo(locDeploymentProps, EMPTY_FIELDS, SPI_EXCLUDED_FIELDS));

        Set<String> memoryEvtStorageProps = new HashSet<>();
        memoryEvtStorageProps.add("expireAgeMs");
        memoryEvtStorageProps.add("expireCount");
        memoryEvtStorageProps.add("filter");
        metadata.put(MemoryEventStorageSpi.class,
            new MetadataInfo(memoryEvtStorageProps, EMPTY_FIELDS, SPI_EXCLUDED_FIELDS));

        Set<String> alwaysFailoverProps = new HashSet<>();
        alwaysFailoverProps.add("maximumFailoverAttempts");
        metadata.put(AlwaysFailoverSpi.class, new MetadataInfo(alwaysFailoverProps, EMPTY_FIELDS, SPI_EXCLUDED_FIELDS));

        Set<String> jobStealingFailoverProps = new HashSet<>();
        jobStealingFailoverProps.add("maximumFailoverAttempts");
        metadata.put(JobStealingFailoverSpi.class,
            new MetadataInfo(jobStealingFailoverProps, EMPTY_FIELDS, SPI_EXCLUDED_FIELDS));

        Set<String> hadoopCfgProps = new HashSet<>();
        hadoopCfgProps.add("mapReducePlanner");
        hadoopCfgProps.add("finishedJobInfoTtl");
        hadoopCfgProps.add("maxParallelTasks");
        hadoopCfgProps.add("maxTaskQueueSize");
        hadoopCfgProps.add("nativeLibraryNames");
        metadata.put(HadoopConfiguration.class, new MetadataInfo(hadoopCfgProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> hadoopWeightMapReduceCfgProps = new HashSet<>();
        hadoopWeightMapReduceCfgProps.add("localMapperWeight");
        hadoopWeightMapReduceCfgProps.add("remoteMapperWeight");
        hadoopWeightMapReduceCfgProps.add("localReducerWeight");
        hadoopWeightMapReduceCfgProps.add("remoteReducerWeight");
        hadoopWeightMapReduceCfgProps.add("preferLocalReducerThresholdWeight");
        metadata.put(IgniteHadoopWeightedMapReducePlanner.class,
            new MetadataInfo(hadoopWeightMapReduceCfgProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> weightedRndLoadBalancingProps = new HashSet<>();
        weightedRndLoadBalancingProps.add("nodeWeight");
        weightedRndLoadBalancingProps.add("useWeights");
        metadata.put(WeightedRandomLoadBalancingSpi.class,
            new MetadataInfo(weightedRndLoadBalancingProps, EMPTY_FIELDS, SPI_EXCLUDED_FIELDS));

        Set<String> adaptiveLoadBalancingProps = new HashSet<>();
        adaptiveLoadBalancingProps.add("loadProbe");
        metadata.put(AdaptiveLoadBalancingSpi.class,
            new MetadataInfo(adaptiveLoadBalancingProps, EMPTY_FIELDS, SPI_EXCLUDED_FIELDS));

        Set<String> roundRobinLoadBalancingProps = new HashSet<>();
        roundRobinLoadBalancingProps.add("perTask");
        metadata.put(RoundRobinLoadBalancingSpi.class,
            new MetadataInfo(roundRobinLoadBalancingProps, EMPTY_FIELDS, SPI_EXCLUDED_FIELDS));

        Set<String> jobCntProbeProps = new HashSet<>();
        jobCntProbeProps.add("useAverage");
        metadata.put(AdaptiveJobCountLoadProbe.class,
            new MetadataInfo(jobCntProbeProps, EMPTY_FIELDS, SPI_EXCLUDED_FIELDS));

        Set<String> cpuLoadProbeProps = new HashSet<>();
        cpuLoadProbeProps.add("useAverage");
        cpuLoadProbeProps.add("useProcessors");
        cpuLoadProbeProps.add("processorCoefficient");
        metadata.put(AdaptiveCpuLoadProbe.class, new MetadataInfo(cpuLoadProbeProps, EMPTY_FIELDS, SPI_EXCLUDED_FIELDS));

        Set<String> adaptiveTimeProbeProps = new HashSet<>();
        adaptiveTimeProbeProps.add("useAverage");
        metadata.put(AdaptiveProcessingTimeLoadProbe.class,
            new MetadataInfo(adaptiveTimeProbeProps, EMPTY_FIELDS, SPI_EXCLUDED_FIELDS));

        Set<String> optimizedMarshallerProps = new HashSet<>();
        optimizedMarshallerProps.add("poolSize");
        optimizedMarshallerProps.add("requireSerializable");

        Set<String> optimizedMarshallerPropsExcl = new HashSet<>();
        optimizedMarshallerPropsExcl.add("context");

        metadata.put(OptimizedMarshaller.class,
            new MetadataInfo(optimizedMarshallerProps, EMPTY_FIELDS, optimizedMarshallerPropsExcl));

        Set<String> memoryCfgProps = new HashSet<>();
        memoryCfgProps.add("pageSize");
        memoryCfgProps.add("concurrencyLevel");
        memoryCfgProps.add("systemCacheInitialSize");
        memoryCfgProps.add("systemCacheMaxSize");
        memoryCfgProps.add("defaultMemoryPolicyName");
        memoryCfgProps.add("defaultMemoryPolicySize");
        memoryCfgProps.add("memoryPolicies");
        metadata.put(MemoryConfiguration.class, new MetadataInfo(EMPTY_FIELDS, memoryCfgProps, EMPTY_FIELDS));

        Set<String> memoryPlcCfgProps = new HashSet<>();
        memoryPlcCfgProps.add("name");
        memoryPlcCfgProps.add("initialSize");
        memoryPlcCfgProps.add("maxSize");
        memoryPlcCfgProps.add("swapFilePath");
        memoryPlcCfgProps.add("pageEvictionMode");
        memoryPlcCfgProps.add("evictionThreshold");
        memoryPlcCfgProps.add("emptyPagesPoolSize");
        memoryPlcCfgProps.add("subIntervals");
        memoryPlcCfgProps.add("rateTimeInterval");
        memoryPlcCfgProps.add("metricsEnabled");
        metadata.put(MemoryPolicyConfiguration.class, new MetadataInfo(EMPTY_FIELDS, memoryPlcCfgProps, EMPTY_FIELDS));

        Set<String> odbcCfgProps = new HashSet<>();
        odbcCfgProps.add("endpointAddress");
        odbcCfgProps.add("socketSendBufferSize");
        odbcCfgProps.add("socketReceiveBufferSize");
        odbcCfgProps.add("maxOpenCursors");
        odbcCfgProps.add("threadPoolSize");
        metadata.put(OdbcConfiguration.class, new MetadataInfo(EMPTY_FIELDS, odbcCfgProps, EMPTY_FIELDS));

        Set<String> persistenceCfgProps = new HashSet<>();
        persistenceCfgProps.add("persistentStorePath");
        persistenceCfgProps.add("metricsEnabled");
        persistenceCfgProps.add("alwaysWriteFullPages");
        persistenceCfgProps.add("checkpointingFrequency");
        persistenceCfgProps.add("checkpointingPageBufferSize");
        persistenceCfgProps.add("checkpointingThreads");
        persistenceCfgProps.add("walStorePath");
        persistenceCfgProps.add("walArchivePath");
        persistenceCfgProps.add("walSegments");
        persistenceCfgProps.add("walSegmentSize");
        persistenceCfgProps.add("walHistorySize");
        persistenceCfgProps.add("walFlushFrequency");
        persistenceCfgProps.add("walFsyncDelayNanos");
        persistenceCfgProps.add("walRecordIteratorBufferSize");
        persistenceCfgProps.add("lockWaitTime");
        persistenceCfgProps.add("rateTimeInterval");
        persistenceCfgProps.add("tlbSize");
        persistenceCfgProps.add("subIntervals");
        persistenceCfgProps.add("walMode");
        persistenceCfgProps.add("walAutoArchiveAfterInactivity");
        persistenceCfgProps.add("writeThrottlingEnabled");
        persistenceCfgProps.add("checkpointWriteOrder");
        persistenceCfgProps.add("fileIOFactory");
        persistenceCfgProps.add("walBufferSize");
        metadata.put(PersistentStoreConfiguration.class,
            new MetadataInfo(EMPTY_FIELDS, persistenceCfgProps, EMPTY_FIELDS));

        Set<String> srvcCfgProps = new HashSet<>();
        srvcCfgProps.add("name");
        srvcCfgProps.add("service");
        srvcCfgProps.add("maxPerNodeCount");
        srvcCfgProps.add("totalCount");
        // Field cache in model.
        srvcCfgProps.add("cacheName");
        srvcCfgProps.add("affinityKey");

        Set<String> srvcCfgPropsExclude = new HashSet<>();
        srvcCfgPropsExclude.add("nodeFilter");

        metadata.put(ServiceConfiguration.class, new MetadataInfo(srvcCfgProps, EMPTY_FIELDS, srvcCfgPropsExclude));

        Set<String> sqlConnectorCfgProps = new HashSet<>();
        sqlConnectorCfgProps.add("host");
        sqlConnectorCfgProps.add("port");
        sqlConnectorCfgProps.add("portRange");
        sqlConnectorCfgProps.add("socketSendBufferSize");
        sqlConnectorCfgProps.add("socketReceiveBufferSize");
        sqlConnectorCfgProps.add("maxOpenCursorsPerConnection");
        sqlConnectorCfgProps.add("threadPoolSize");
        sqlConnectorCfgProps.add("tcpNoDelay");
        metadata.put(SqlConnectorConfiguration.class,
            new MetadataInfo(EMPTY_FIELDS, sqlConnectorCfgProps, EMPTY_FIELDS));

        Set<String> sslCfgProps = new HashSet<>();
        sslCfgProps.add("keyAlgorithm");
        sslCfgProps.add("keyStoreFilePath");
        // Only on code generation.
        sslCfgProps.add("keyStorePassword");
        sslCfgProps.add("keyStoreType");
        sslCfgProps.add("protocol");
        sslCfgProps.add("trustManagers");
        sslCfgProps.add("trustStoreFilePath");
        // Only on code generation.
        sslCfgProps.add("trustStorePassword");
        sslCfgProps.add("trustStoreType");
        sslCfgProps.add("cipherSuites");
        sslCfgProps.add("protocols");
        metadata.put(SslContextFactory.class, new MetadataInfo(sslCfgProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> executorProps = new HashSet<>();
        executorProps.add("name");
        executorProps.add("size");
        metadata.put(ExecutorConfiguration.class, new MetadataInfo(executorProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> transactionCfgProps = new HashSet<>();
        transactionCfgProps.add("defaultTxConcurrency");
        transactionCfgProps.add("defaultTxIsolation");
        transactionCfgProps.add("defaultTxTimeout");
        transactionCfgProps.add("pessimisticTxLogLinger");
        transactionCfgProps.add("pessimisticTxLogSize");
        transactionCfgProps.add("txManagerFactory");
        transactionCfgProps.add("deadlockTimeout");
        transactionCfgProps.add("useJtaSynchronization");
        transactionCfgProps.add("txTimeoutOnPartitionMapExchange");

        Set<String> transactionCfgPropsDep = new HashSet<>();
        transactionCfgPropsDep.add("txSerializableEnabled");
        transactionCfgPropsDep.add("txManagerLookupClassName");
        metadata.put(TransactionConfiguration.class,
            new MetadataInfo(transactionCfgProps, transactionCfgPropsDep, EMPTY_FIELDS));

        // Cache configuration.

        Set<String> cacheCfgProps = new HashSet<>();
        cacheCfgProps.add("name");
        cacheCfgProps.add("groupName");
        cacheCfgProps.add("cacheMode");
        cacheCfgProps.add("atomicityMode");
        cacheCfgProps.add("backups");
        cacheCfgProps.add("partitionLossPolicy");
        cacheCfgProps.add("readFromBackup");
        cacheCfgProps.add("copyOnRead");
        cacheCfgProps.add("invalidate");
        cacheCfgProps.add("affinityMapper");
        cacheCfgProps.add("topologyValidator");
        cacheCfgProps.add("maxConcurrentAsyncOperations");
        cacheCfgProps.add("defaultLockTimeout");
        cacheCfgProps.add("writeSynchronizationMode");
        cacheCfgProps.add("onheapCacheEnabled");
        cacheCfgProps.add("dataRegionName");
        // Removed since 2.0.
        // cacheCfgProps.add("memoryMode");
        // cacheCfgProps.add("offHeapMode");
        // cacheCfgProps.add("offHeapMaxMemory");
        cacheCfgProps.add("evictionPolicyFactory");
        cacheCfgProps.add("evictionFilter");
        // Removed since 2.0.
        // cacheCfgProps.add("startSize");
        // cacheCfgProps.add("swapEnabled");
        cacheCfgProps.add("nearConfiguration");
        cacheCfgProps.add("sqlSchema");
        // Removed since 2.0.
        // cacheCfgProps.add("sqlOnheapRowCacheSize");
        cacheCfgProps.add("queryDetailMetricsSize");
        cacheCfgProps.add("sqlFunctionClasses");
        // Removed since 2.0
        // cacheCfgProps.add("snapshotableIndex");
        cacheCfgProps.add("sqlEscapeAll");
        cacheCfgProps.add("queryParallelism");
        cacheCfgProps.add("rebalanceMode");
        cacheCfgProps.add("rebalanceBatchSize");
        cacheCfgProps.add("rebalanceBatchesPrefetchCount");
        cacheCfgProps.add("rebalanceOrder");
        cacheCfgProps.add("rebalanceDelay");
        cacheCfgProps.add("rebalanceTimeout");
        cacheCfgProps.add("rebalanceThrottle");
        cacheCfgProps.add("statisticsEnabled");
        cacheCfgProps.add("managementEnabled");
        cacheCfgProps.add("cacheStoreFactory");
        cacheCfgProps.add("storeKeepBinary");
        cacheCfgProps.add("loadPreviousValue");
        cacheCfgProps.add("readThrough");
        cacheCfgProps.add("writeThrough");
        cacheCfgProps.add("writeBehindEnabled");
        cacheCfgProps.add("writeBehindBatchSize");
        cacheCfgProps.add("writeBehindFlushSize");
        cacheCfgProps.add("writeBehindFlushFrequency");
        cacheCfgProps.add("writeBehindFlushThreadCount");
        cacheCfgProps.add("writeBehindCoalescing");
        cacheCfgProps.add("indexedTypes");
        cacheCfgProps.add("queryEntities");
        cacheCfgProps.add("pluginConfigurations");
        cacheCfgProps.add("cacheWriterFactory");
        cacheCfgProps.add("cacheLoaderFactory");
        cacheCfgProps.add("expiryPolicyFactory");
        cacheCfgProps.add("storeConcurrentLoadAllThreshold");
        cacheCfgProps.add("sqlIndexMaxInlineSize");
        cacheCfgProps.add("sqlOnheapCacheEnabled");
        cacheCfgProps.add("sqlOnheapCacheMaxSize");
        cacheCfgProps.add("diskPageCompression");
        cacheCfgProps.add("diskPageCompressionLevel");
        cacheCfgProps.add("interceptor");
        cacheCfgProps.add("storeByValue");
        cacheCfgProps.add("eagerTtl");
        cacheCfgProps.add("encryptionEnabled");
        cacheCfgProps.add("eventsDisabled");
        cacheCfgProps.add("maxQueryIteratorsCount");
        cacheCfgProps.add("keyConfiguration");
        cacheCfgProps.add("cacheStoreSessionListenerFactories");
        cacheCfgProps.add("affinity");

        Set<String> cacheCfgPropsDep = new HashSet<>();
        // Removed since 2.0.
        // cacheCfgPropsDep.add("atomicWriteOrderMode");
        cacheCfgPropsDep.add("memoryPolicyName");
        cacheCfgPropsDep.add("longQueryWarningTimeout");
        cacheCfgPropsDep.add("rebalanceThreadPoolSize");
        cacheCfgPropsDep.add("transactionManagerLookupClassName");
        cacheCfgPropsDep.add("evictionPolicy");

        Set<String> cacheCfgPropsExcl = new HashSet<>();
        cacheCfgPropsExcl.add("nodeFilter");
        cacheCfgPropsExcl.add("types");

        metadata.put(CacheConfiguration.class, new MetadataInfo(cacheCfgProps, cacheCfgPropsDep, cacheCfgPropsExcl));

        Set<String> rendezvousAffinityProps = new HashSet<>();
        rendezvousAffinityProps.add("partitions");
        rendezvousAffinityProps.add("affinityBackupFilter");
        rendezvousAffinityProps.add("excludeNeighbors");

        Set<String> rendezvousAffinityPropsDep = new HashSet<>();
        rendezvousAffinityPropsDep.add("backupFilter");
        metadata.put(RendezvousAffinityFunction.class,
            new MetadataInfo(rendezvousAffinityProps, rendezvousAffinityPropsDep, EMPTY_FIELDS));

        Set<String> nearCfgProps = new HashSet<>();
        nearCfgProps.add("nearStartSize");
        nearCfgProps.add("nearEvictionPolicyFactory");

        Set<String> nearCfgPropsDep = new HashSet<>();
        nearCfgPropsDep.add("nearEvictionPolicy");

        metadata.put(NearCacheConfiguration.class, new MetadataInfo(nearCfgProps, nearCfgPropsDep, EMPTY_FIELDS));

        Set<String> jdbcPojoStoreProps = new HashSet<>();
        // Only setter for dataSource field.
        // jdbcPojoStoreProps.add("dataSourceBean");
        jdbcPojoStoreProps.add("dialect");
        jdbcPojoStoreProps.add("batchSize");
        jdbcPojoStoreProps.add("maximumPoolSize");
        jdbcPojoStoreProps.add("maximumWriteAttempts");
        jdbcPojoStoreProps.add("parallelLoadCacheMinimumThreshold");
        jdbcPojoStoreProps.add("hasher");
        jdbcPojoStoreProps.add("transformer");
        jdbcPojoStoreProps.add("sqlEscapeAll");
        jdbcPojoStoreProps.add("types");

        // Configured via dataSource property.
        Set<String> jdbcPojoStorePropsExcl = new HashSet<>();
        jdbcPojoStorePropsExcl.add("dataSourceBean");
        jdbcPojoStorePropsExcl.add("dataSourceFactory");

        metadata.put(CacheJdbcPojoStoreFactory.class,
            new MetadataInfo(jdbcPojoStoreProps, EMPTY_FIELDS, jdbcPojoStorePropsExcl));

        Set<String> jdbcBlobStoreProps = new HashSet<>();
        jdbcBlobStoreProps.add("connectionUrl");
        jdbcBlobStoreProps.add("user");
        // Only setter for dataSource.
        // jdbcBlobStoreProps.add("dataSourceBean");
        // jdbcBlobStoreProps.add("dialect");
        jdbcBlobStoreProps.add("initSchema");
        jdbcBlobStoreProps.add("createTableQuery");
        jdbcBlobStoreProps.add("loadQuery");
        jdbcBlobStoreProps.add("insertQuery");
        jdbcBlobStoreProps.add("updateQuery");
        jdbcBlobStoreProps.add("deleteQuery");
        metadata.put(CacheJdbcBlobStore.class, new MetadataInfo(jdbcBlobStoreProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> hibernateBlobStoreProps = new HashSet<>();
        hibernateBlobStoreProps.add("hibernateProperties");
        metadata.put(CacheHibernateBlobStore.class,
            new MetadataInfo(hibernateBlobStoreProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> igfsCfgProps = new HashSet<>();
        igfsCfgProps.add("name");
        igfsCfgProps.add("defaultMode");
        // Removed since 2.0.
        // igfsCfgProps.add("dualModeMaxPendingPutsSize");
        // igfsCfgProps.add("dualModePutExecutorService");
        // igfsCfgProps.add("dualModePutExecutorServiceShutdown");
        igfsCfgProps.add("fragmentizerEnabled");
        igfsCfgProps.add("fragmentizerConcurrentFiles");
        igfsCfgProps.add("fragmentizerThrottlingBlockLength");
        igfsCfgProps.add("fragmentizerThrottlingDelay");
        igfsCfgProps.add("ipcEndpointEnabled");
        igfsCfgProps.add("ipcEndpointConfiguration");
        igfsCfgProps.add("blockSize");
        // streamBufferSize field in model.
        igfsCfgProps.add("bufferSize");
        // Removed since 2.0.
        // igfsCfgProps.add("streamBufferSize");
        // igfsCfgProps.add("maxSpaceSize");
        igfsCfgProps.add("maximumTaskRangeLength");
        igfsCfgProps.add("managementPort");
        igfsCfgProps.add("perNodeBatchSize");
        igfsCfgProps.add("perNodeParallelBatchCount");
        igfsCfgProps.add("prefetchBlocks");
        igfsCfgProps.add("sequentialReadsBeforePrefetch");
        // Removed since 2.0.
        // igfsCfgProps.add("trashPurgeTimeout");
        igfsCfgProps.add("colocateMetadata");
        igfsCfgProps.add("relaxedConsistency");
        igfsCfgProps.add("updateFileLengthOnFlush");
        igfsCfgProps.add("pathModes");
        igfsCfgProps.add("secondaryFileSystem");

        Set<String> igfsCfgPropsExclude = new HashSet<>();
        igfsCfgPropsExclude.add("dataCacheConfiguration");
        igfsCfgPropsExclude.add("metaCacheConfiguration");

        metadata.put(FileSystemConfiguration.class, new MetadataInfo(igfsCfgProps, EMPTY_FIELDS, igfsCfgPropsExclude));

        Set<String> igfsBlocMapperProps = new HashSet<>();
        igfsBlocMapperProps.add("groupSize");

        metadata.put(IgfsGroupDataBlocksKeyMapper.class,
            new MetadataInfo(igfsBlocMapperProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> secHadoopIgfsCfgProps = new HashSet<>();
        secHadoopIgfsCfgProps.add("defaultUserName");
        secHadoopIgfsCfgProps.add("fileSystemFactory");

        metadata.put(IgniteHadoopIgfsSecondaryFileSystem.class, new MetadataInfo(secHadoopIgfsCfgProps, EMPTY_FIELDS,
            EMPTY_FIELDS));

        Set<String> cachingIgfsCfgProps = new HashSet<>();
        cachingIgfsCfgProps.add("uri");
        cachingIgfsCfgProps.add("configPaths");
        cachingIgfsCfgProps.add("userNameMapper");

        metadata.put(CachingHadoopFileSystemFactory.class,
            new MetadataInfo(cachingIgfsCfgProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> kerberosIgfsCfgProps = new HashSet<>();
        kerberosIgfsCfgProps.add("uri");
        kerberosIgfsCfgProps.add("configPaths");
        kerberosIgfsCfgProps.add("userNameMapper");
        kerberosIgfsCfgProps.add("keyTab");
        kerberosIgfsCfgProps.add("keyTabPrincipal");
        kerberosIgfsCfgProps.add("reloginInterval");

        metadata.put(KerberosHadoopFileSystemFactory.class, new MetadataInfo(kerberosIgfsCfgProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> chainedIgfsUsrNameMapperProps = new HashSet<>();
        chainedIgfsUsrNameMapperProps.add("mappers");

        metadata.put(ChainedUserNameMapper.class, new MetadataInfo(chainedIgfsUsrNameMapperProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> basicIgfsUsrNameMapperProps = new HashSet<>();
        basicIgfsUsrNameMapperProps.add("defaultUserName");
        basicIgfsUsrNameMapperProps.add("useDefaultUserName");
        basicIgfsUsrNameMapperProps.add("mappings");

        metadata.put(BasicUserNameMapper.class, new MetadataInfo(basicIgfsUsrNameMapperProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> kerberosIgfsUsrNameMapperProps = new HashSet<>();
        kerberosIgfsUsrNameMapperProps.add("instance");
        kerberosIgfsUsrNameMapperProps.add("realm");

        metadata.put(KerberosUserNameMapper.class, new MetadataInfo(kerberosIgfsUsrNameMapperProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> ipcEndpointProps = new HashSet<>();
        ipcEndpointProps.add("type");
        ipcEndpointProps.add("host");
        ipcEndpointProps.add("port");
        ipcEndpointProps.add("memorySize");
        ipcEndpointProps.add("threadCount");
        ipcEndpointProps.add("tokenDirectoryPath");
        metadata.put(IgfsIpcEndpointConfiguration.class, new MetadataInfo(ipcEndpointProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> qryEntityProps = new HashSet<>();
        qryEntityProps.add("keyType");
        qryEntityProps.add("valueType");
        qryEntityProps.add("aliases");
        qryEntityProps.add("fields");
        qryEntityProps.add("indexes");
        qryEntityProps.add("tableName");
        qryEntityProps.add("keyFieldName");
        qryEntityProps.add("valueFieldName");
        qryEntityProps.add("keyFields");
        qryEntityProps.add("fieldsPrecision");
        qryEntityProps.add("notNullFields");
        qryEntityProps.add("fieldsScale");
        qryEntityProps.add("defaultFieldValues");
        metadata.put(QueryEntity.class, new MetadataInfo(qryEntityProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> qryIdxProps = new HashSet<>();
        qryIdxProps.add("name");
        qryIdxProps.add("indexType");
        qryIdxProps.add("fields");
        qryIdxProps.add("inlineSize");

        Set<String> qryIdxPropsExcl = new HashSet<>();
        qryIdxPropsExcl.add("fieldNames");

        metadata.put(QueryIndex.class, new MetadataInfo(qryIdxProps, EMPTY_FIELDS, qryIdxPropsExcl));

        Set<String> jdbcTypeProps = new HashSet<>();
        jdbcTypeProps.add("cacheName");
        jdbcTypeProps.add("keyType");
        jdbcTypeProps.add("valueType");
        jdbcTypeProps.add("databaseSchema");
        jdbcTypeProps.add("databaseTable");
        jdbcTypeProps.add("keyFields");
        jdbcTypeProps.add("valueFields");

        metadata.put(JdbcType.class, new MetadataInfo(jdbcTypeProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> sorterEvictionProps = new HashSet<>();
        sorterEvictionProps.add("batchSize");
        sorterEvictionProps.add("maxMemorySize");
        sorterEvictionProps.add("maxSize");
        metadata.put(SortedEvictionPolicy.class, new MetadataInfo(sorterEvictionProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> lruEvictionProps = new HashSet<>();
        lruEvictionProps.add("batchSize");
        lruEvictionProps.add("maxMemorySize");
        lruEvictionProps.add("maxSize");
        metadata.put(LruEvictionPolicy.class, new MetadataInfo(lruEvictionProps, EMPTY_FIELDS, EMPTY_FIELDS));

        Set<String> fifoEvictionProps = new HashSet<>();
        fifoEvictionProps.add("batchSize");
        fifoEvictionProps.add("maxMemorySize");
        fifoEvictionProps.add("maxSize");
        metadata.put(FifoEvictionPolicy.class, new MetadataInfo(fifoEvictionProps, EMPTY_FIELDS, EMPTY_FIELDS));
    }

    /**
     * Check an accordance of possible to configure properties and configuration classes.
     */
    @Test
    public void testConfiguration() {
        prepareMetadata();

        HashMap<Class<?>, WrongFields> diff = new HashMap<>();

        for(Map.Entry<Class<?>, MetadataInfo> ent: metadata.entrySet()) {
            Class<?> cls = ent.getKey();
            MetadataInfo meta = ent.getValue();

            Set<String> props = meta.getGeneratedFields();
            Set<String> knownDeprecated = meta.getDeprecatedFields();
            Set<String> excludeFields = meta.getExcludedFields();

            boolean clsDeprecated = cls.getAnnotation(Deprecated.class) != null;
            Map<String, FieldProcessingInfo> clsProps = new HashMap<>();

            for (Method m: cls.getMethods()) {
                String mtdName = m.getName();

                String propName = mtdName.length() > 3 && (mtdName.startsWith("get") || mtdName.startsWith("set")) ?
                    mtdName.toLowerCase().charAt(3) + mtdName.substring(4) :
                    mtdName.length() > 2 && mtdName.startsWith("is") ?
                        mtdName.toLowerCase().charAt(2) + mtdName.substring(3) : null;

                boolean deprecated = clsDeprecated || m.getAnnotation(Deprecated.class) != null;

                if (propName != null && !excludeFields.contains(propName)) {
                    clsProps.put(propName,
                        clsProps
                            .getOrDefault(propName, new FieldProcessingInfo(propName, 0, deprecated))
                            .deprecated(deprecated)
                            .next());
                }
            }

            Set<String> missedFields = new HashSet<>();
            Set<String> deprecatedFields = new HashSet<>();

            for(Map.Entry<String, FieldProcessingInfo> e: clsProps.entrySet()) {
                String prop = e.getKey();
                FieldProcessingInfo info = e.getValue();

                if (info.getOccurrence() > 1 && !info.isDeprecated() && !props.contains(prop))
                    missedFields.add(prop);

                if (info.getOccurrence() > 1 && info.isDeprecated() && !props.contains(prop) && !knownDeprecated.contains(prop))
                    deprecatedFields.add(prop);
            }

            Set<String> rmvFields = new HashSet<>();

            for (String p: props)
                if (!clsProps.containsKey(p))
                    rmvFields.add(p);

            for (String p: knownDeprecated)
                if (!clsProps.containsKey(p))
                    rmvFields.add(p);

            WrongFields fields = new WrongFields(missedFields, deprecatedFields, rmvFields);

            if (fields.nonEmpty()) {
                diff.put(cls, fields);

                log("Result for class: " + cls.getName());

                if (!missedFields.isEmpty()) {
                    log("  Missed");

                    for (String fld: missedFields)
                        log("    " + fld);
                }

                if (!deprecatedFields.isEmpty()) {
                    log("  Deprecated");

                    for (String fld: deprecatedFields)
                        log("    " + fld);
                }

                if (!rmvFields.isEmpty()) {
                    log("  Removed");

                    for (String fld: rmvFields)
                        log("    " + fld);
                }

                log("");
            }
        }

        // Test will pass only if no difference found between IgniteConfiguration and Web Console generated configuration.
        assert diff.isEmpty() : "Found difference between IgniteConfiguration and Web Console";
    }
}
