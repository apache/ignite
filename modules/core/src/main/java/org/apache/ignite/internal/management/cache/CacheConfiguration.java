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

package org.apache.ignite.internal.management.cache;

import java.util.List;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactIterable;

/**
 * Data transfer object for cache configuration properties.
 */
public class CacheConfiguration extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    @Order(0)
    String name;

    /** Cache group name. */
    @Order(1)
    String grpName;

    /** Cache mode. */
    @Order(2)
    CacheMode mode;

    /** Cache atomicity mode. */
    @Order(3)
    CacheAtomicityMode atomicityMode;

    /** Eager ttl flag. */
    @Order(4)
    boolean eagerTtl;

    /** Write synchronization mode. */
    @Order(5)
    CacheWriteSynchronizationMode writeSynchronizationMode;

    /** Invalidate. */
    @Order(6)
    boolean invalidate;

    /** Max concurrent async operations. */
    @Order(7)
    int maxConcurrentAsyncOps;

    /** Cache interceptor. */
    @Order(8)
    String interceptor;

    /** Default lock acquisition timeout. */
    @Order(9)
    long dfltLockTimeout;

    /** Cache affinity config. */
    @Order(10)
    CacheAffinityConfiguration affinityCfg;

    /** Preload config. */
    @Order(11)
    CacheRebalanceConfiguration rebalanceCfg;

    /** Eviction config. */
    @Order(12)
    CacheEvictionConfiguration evictCfg;

    /** Near cache config. */
    @Order(13)
    CacheNearConfiguration nearCfg;

    /** Store config. */
    @Order(14)
    CacheStoreConfiguration storeCfg;

    /** Collection of query entities. */
    @Order(15)
    List<QueryEntity> qryEntities;

    /** Collection of type metadata. */
    @Order(16)
    List<CacheJdbcType> jdbcTypes;

    /** Whether statistics collection is enabled. */
    @Order(17)
    boolean statisticsEnabled;

    /** Whether management is enabled. */
    @Order(18)
    boolean mgmtEnabled;

    /** Class name of cache loader factory. */
    @Order(19)
    String ldrFactory;

    /** Class name of cache writer factory. */
    @Order(20)
    String writerFactory;

    /** Class name of expiry policy factory. */
    @Order(21)
    String expiryPlcFactory;

    /** Query configuration. */
    @Order(22)
    QueryConfiguration qryCfg;

    /** System cache flag. */
    @Order(23)
    boolean sys;

    /** Keep binary in store flag. */
    @Order(24)
    boolean storeKeepBinary;

    /** On-heap cache enabled flag. */
    @Order(25)
    boolean onheapCache;

    /** Partition loss policy. */
    @Order(26)
    PartitionLossPolicy partLossPlc;

    /** Query parallelism. */
    @Order(27)
    int qryParallelism;

    /** Copy on read flag. */
    @Order(28)
    boolean cpOnRead;

    /** Eviction filter. */
    @Order(29)
    String evictFilter;

    /** Listener configurations. */
    @Order(30)
    String lsnrConfigurations;

    /** */
    @Order(31)
    boolean loadPrevVal;

    /** Name of {@link DataRegionConfiguration} for this cache */
    @Order(32)
    String dataRegName;

    /** Maximum inline size for sql indexes. */
    @Order(33)
    int sqlIdxMaxInlineSize;

    /** Node filter specifying nodes on which this cache should be deployed. */
    @Order(34)
    String nodeFilter;

    /** */
    @Order(35)
    int qryDetailMetricsSz;

    /** Flag indicating whether data can be read from backup. */
    @Order(36)
    boolean readFromBackup;

    /** Name of class implementing GridCacheTmLookup. */
    @Order(37)
    String tmLookupClsName;

    /** Cache topology validator. */
    @Order(38)
    String topValidator;

    /** Dynamic deployment ID. */
    @Order(39)
    IgniteUuid dynamicDeploymentId;

    /** Disk page compression algorithm. */
    @Order(40)
    DiskPageCompression diskPageCompression;

    /** Algorithm specific disk page compression level. */
    @Order(41)
    Integer diskPageCompressionLevel;

    /**
     * Default constructor.
     */
    public CacheConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for cache configuration properties.
     *
     * @param ignite Grid.
     * @param ccfg Cache configuration.
     * @param dynamicDeploymentId Dynamic deployment ID.
     */
    public CacheConfiguration(IgniteEx ignite, org.apache.ignite.configuration.CacheConfiguration ccfg, IgniteUuid dynamicDeploymentId) {
        name = ccfg.getName();
        grpName = ccfg.getGroupName();
        this.dynamicDeploymentId = dynamicDeploymentId;
        mode = ccfg.getCacheMode();
        atomicityMode = ccfg.getAtomicityMode();
        eagerTtl = ccfg.isEagerTtl();
        writeSynchronizationMode = ccfg.getWriteSynchronizationMode();
        invalidate = ccfg.isInvalidate();
        maxConcurrentAsyncOps = ccfg.getMaxConcurrentAsyncOperations();
        interceptor = compactClass(ccfg.getInterceptor());
        dfltLockTimeout = ccfg.getDefaultLockTimeout();
        qryEntities = QueryEntity.list(ccfg.getQueryEntities());
        jdbcTypes = CacheJdbcType.list(ccfg.getCacheStoreFactory());
        statisticsEnabled = ccfg.isStatisticsEnabled();
        mgmtEnabled = ccfg.isManagementEnabled();
        ldrFactory = compactClass(ccfg.getCacheLoaderFactory());
        writerFactory = compactClass(ccfg.getCacheWriterFactory());
        expiryPlcFactory = compactClass(ccfg.getExpiryPolicyFactory());

        sys = ignite.context().cache().systemCache(ccfg.getName());
        storeKeepBinary = ccfg.isStoreKeepBinary();
        onheapCache = ccfg.isOnheapCacheEnabled();
        partLossPlc = ccfg.getPartitionLossPolicy();
        qryParallelism = ccfg.getQueryParallelism();

        affinityCfg = new CacheAffinityConfiguration(ccfg);
        rebalanceCfg = new CacheRebalanceConfiguration(ccfg);
        evictCfg = new CacheEvictionConfiguration(ccfg);
        nearCfg = new CacheNearConfiguration(ccfg);

        storeCfg = new CacheStoreConfiguration(ignite, ccfg);

        qryCfg = new QueryConfiguration(ccfg);

        cpOnRead = ccfg.isCopyOnRead();
        evictFilter = compactClass(ccfg.getEvictionFilter());
        lsnrConfigurations = compactIterable(ccfg.getCacheEntryListenerConfigurations());
        loadPrevVal = ccfg.isLoadPreviousValue();
        dataRegName = ccfg.getDataRegionName();
        sqlIdxMaxInlineSize = ccfg.getSqlIndexMaxInlineSize();
        nodeFilter = compactClass(ccfg.getNodeFilter());
        qryDetailMetricsSz = ccfg.getQueryDetailMetricsSize();
        readFromBackup = ccfg.isReadFromBackup();
        tmLookupClsName = ccfg.getTransactionManagerLookupClassName();
        topValidator = compactClass(ccfg.getTopologyValidator());

        diskPageCompression = ccfg.getDiskPageCompression();
        diskPageCompressionLevel = ccfg.getDiskPageCompressionLevel();
    }

    /**
     * @return Cache name.
     */
    @Nullable public String getName() {
        return name;
    }

    /**
     * @return Cache group name.
     */
    @Nullable public String getGroupName() {
        return grpName;
    }

    /**
     * @return Cache mode.
     */
    public CacheMode getMode() {
        return mode;
    }

    /**
     * @return Cache atomicity mode.
     */
    public CacheAtomicityMode getAtomicityMode() {
        return atomicityMode;
    }

    /**
     * @return Eager ttl flag
     */
    public boolean eagerTtl() {
        return eagerTtl;
    }

    /**
     * @return Write synchronization mode.
     */
    public CacheWriteSynchronizationMode getWriteSynchronizationMode() {
        return writeSynchronizationMode;
    }

    /**
     * @return Invalidate.
     */
    public boolean isInvalidate() {
        return invalidate;
    }

    /**
     * @return Max concurrent async operations
     */
    public int getMaxConcurrentAsyncOperations() {
        return maxConcurrentAsyncOps;
    }

    /**
     * @return Cache interceptor.
     */
    @Nullable public String getInterceptor() {
        return interceptor;
    }

    /**
     * @return Gets default lock acquisition timeout.
     */
    public long getDefaultLockTimeout() {
        return dfltLockTimeout;
    }

    /**
     * @return Collection of type metadata.
     */
    public List<CacheJdbcType> getJdbcTypes() {
        return jdbcTypes;
    }

    /**
     * @return Near cache config.
     */
    public CacheNearConfiguration getNearConfiguration() {
        return nearCfg;
    }

    /**
     * @return Eager ttl flag.
     */
    public boolean isEagerTtl() {
        return eagerTtl;
    }

    /**
     * @return {@code true} if cache statistics collection enabled.
     */
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * @return Whether management is enabled.
     */
    public boolean isManagementEnabled() {
        return mgmtEnabled;
    }

    /**
     * @return Class name of cache loader factory.
     */
    public String getLoaderFactory() {
        return ldrFactory;
    }

    /**
     * @return Class name of cache writer factory.
     */
    public String getWriterFactory() {
        return writerFactory;
    }

    /**
     * @return Class name of expiry policy factory.
     */
    public String getExpiryPolicyFactory() {
        return expiryPlcFactory;
    }

    /**
     * @return Cache affinity config.
     */
    public CacheAffinityConfiguration getAffinityConfiguration() {
        return affinityCfg;
    }

    /**
     * @return Preload config.
     */
    public CacheRebalanceConfiguration getRebalanceConfiguration() {
        return rebalanceCfg;
    }

    /**
     * @return Eviction config.
     */
    public CacheEvictionConfiguration getEvictionConfiguration() {
        return evictCfg;
    }

    /**
     * @return Store config
     */
    public CacheStoreConfiguration getStoreConfiguration() {
        return storeCfg;
    }

    /**
     * @return Collection of query entities.
     */
    public List<QueryEntity> getQueryEntities() {
        return qryEntities;
    }

    /**
     * @return Collection of query entities.
     */
    public QueryConfiguration getQueryConfiguration() {
        return qryCfg;
    }

    /**
     * @return System cache flag.
     */
    public boolean isSystem() {
        return sys;
    }

    /**
     * @return Keep binary in store flag.
     */
    public Boolean isStoreKeepBinary() {
        return storeKeepBinary;
    }

    /**
     * @return On-heap cache enabled flag.
     */
    public boolean isOnheapCacheEnabled() {
        return onheapCache;
    }

    /**
     * @return Partition loss policy.
     */
    public PartitionLossPolicy getPartitionLossPolicy() {
        return partLossPlc;
    }

    /**
     * @return Query parallelism.
     */
    public int getQueryParallelism() {
        return qryParallelism;
    }

    /**
     * @return Copy on read flag.
     */
    public boolean isCopyOnRead() {
        return cpOnRead;
    }

    /**
     * @return Eviction filter or {@code null}.
     */
    public String getEvictionFilter() {
        return evictFilter;
    }

    /**
     * @return Listener configurations.
     */
    public String getListenerConfigurations() {
        return lsnrConfigurations;
    }

    /**
     * @return Load previous value flag.
     */
    public boolean isLoadPreviousValue() {
        return loadPrevVal;
    }

    /**
     * @return {@link DataRegionConfiguration} name.
     */
    @Deprecated
    public String getMemoryPolicyName() {
        return dataRegName;
    }

    /**
     * @return Maximum payload size for offheap indexes.
     */
    public int getSqlIndexMaxInlineSize() {
        return sqlIdxMaxInlineSize;
    }

    /**
     * @return Predicate specifying on which nodes the cache should be started.
     */
    public String getNodeFilter() {
        return nodeFilter;
    }

    /**
     * @return Maximum number of query metrics that will be stored in memory.
     */
    public int getQueryDetailMetricsSize() {
        return qryDetailMetricsSz;
    }

    /**
     * @return {@code true} if data can be read from backup node or {@code false} if data always should be read from
     * primary node and never from backup.
     */
    public boolean isReadFromBackup() {
        return readFromBackup;
    }

    /**
     * @return Transaction manager finder.
     */
    @Deprecated
    public String getTransactionManagerLookupClassName() {
        return tmLookupClsName;
    }

    /**
     * @return Topology validator.
     */
    public String getTopologyValidator() {
        return topValidator;
    }

    /**
     * @return Cache dynamic deployment ID.
     */
    public IgniteUuid getDynamicDeploymentId() {
        return dynamicDeploymentId;
    }

    /**
     * @return Disk page compression algorithm.
     */
    public DiskPageCompression getDiskPageCompression() {
        return diskPageCompression;
    }

    /**
     * @return Algorithm specific disk page compression level.
     */
    public Integer getDiskPageCompressionLevel() {
        return diskPageCompressionLevel;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheConfiguration.class, this);
    }
}
