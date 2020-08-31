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

package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.apache.ignite.internal.visor.query.VisorQueryConfiguration;
import org.apache.ignite.internal.visor.query.VisorQueryEntity;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactIterable;

/**
 * Data transfer object for cache configuration properties.
 */
public class VisorCacheConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String name;

    /** Cache group name. */
    private String grpName;

    /** Cache mode. */
    private CacheMode mode;

    /** Cache atomicity mode. */
    private CacheAtomicityMode atomicityMode;

    /** Eager ttl flag. */
    private boolean eagerTtl;

    /** Write synchronization mode. */
    private CacheWriteSynchronizationMode writeSynchronizationMode;

    /** Invalidate. */
    private boolean invalidate;

    /** Max concurrent async operations. */
    private int maxConcurrentAsyncOps;

    /** Cache interceptor. */
    private String interceptor;

    /** Default lock acquisition timeout. */
    private long dfltLockTimeout;

    /** Cache affinity config. */
    private VisorCacheAffinityConfiguration affinityCfg;

    /** Preload config. */
    private VisorCacheRebalanceConfiguration rebalanceCfg;

    /** Eviction config. */
    private VisorCacheEvictionConfiguration evictCfg;

    /** Near cache config. */
    private VisorCacheNearConfiguration nearCfg;

    /** Store config. */
    private VisorCacheStoreConfiguration storeCfg;

    /** Collection of query entities. */
    private List<VisorQueryEntity> qryEntities;

    /** Collection of type metadata. */
    private List<VisorCacheJdbcType> jdbcTypes;

    /** Whether statistics collection is enabled. */
    private boolean statisticsEnabled;

    /** Whether management is enabled. */
    private boolean mgmtEnabled;

    /** Class name of cache loader factory. */
    private String ldrFactory;

    /** Class name of cache writer factory. */
    private String writerFactory;

    /** Class name of expiry policy factory. */
    private String expiryPlcFactory;

    /** Query configuration. */
    private VisorQueryConfiguration qryCfg;

    /** System cache flag. */
    private boolean sys;

    /** Keep binary in store flag. */
    private boolean storeKeepBinary;

    /** On-heap cache enabled flag. */
    private boolean onheapCache;

    /** Partition loss policy. */
    private PartitionLossPolicy partLossPlc;

    /** Query parallelism. */
    private int qryParallelism;

    /** Copy on read flag. */
    private boolean cpOnRead;

    /** Eviction filter. */
    private String evictFilter;

    /** Listener configurations. */
    private String lsnrConfigurations;

    /** */
    private boolean loadPrevVal;

    /** Name of {@link DataRegionConfiguration} for this cache */
    private String dataRegName;

    /** Maximum inline size for sql indexes. */
    private int sqlIdxMaxInlineSize;

    /** Node filter specifying nodes on which this cache should be deployed. */
    private String nodeFilter;

    /** */
    private int qryDetailMetricsSz;

    /** Flag indicating whether data can be read from backup. */
    private boolean readFromBackup;

    /** Name of class implementing GridCacheTmLookup. */
    private String tmLookupClsName;

    /** Cache topology validator. */
    private String topValidator;

    /** Dynamic deployment ID. */
    private IgniteUuid dynamicDeploymentId;

    /** Disk page compression algorithm. */
    private DiskPageCompression diskPageCompression;

    /** Algorithm specific disk page compression level. */
    private Integer diskPageCompressionLevel;

    /**
     * Default constructor.
     */
    public VisorCacheConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for cache configuration properties.
     *
     * @param ignite Grid.
     * @param ccfg Cache configuration.
     * @param dynamicDeploymentId Dynamic deployment ID.
     */
    public VisorCacheConfiguration(IgniteEx ignite, CacheConfiguration ccfg, IgniteUuid dynamicDeploymentId) {
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
        qryEntities = VisorQueryEntity.list(ccfg.getQueryEntities());
        jdbcTypes = VisorCacheJdbcType.list(ccfg.getCacheStoreFactory());
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

        affinityCfg = new VisorCacheAffinityConfiguration(ccfg);
        rebalanceCfg = new VisorCacheRebalanceConfiguration(ccfg);
        evictCfg = new VisorCacheEvictionConfiguration(ccfg);
        nearCfg = new VisorCacheNearConfiguration(ccfg);

        storeCfg = new VisorCacheStoreConfiguration(ignite, ccfg);

        qryCfg = new VisorQueryConfiguration(ccfg);

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
    public List<VisorCacheJdbcType> getJdbcTypes() {
        return jdbcTypes;
    }

    /**
     * @return Near cache config.
     */
    public VisorCacheNearConfiguration getNearConfiguration() {
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
    public VisorCacheAffinityConfiguration getAffinityConfiguration() {
        return affinityCfg;
    }

    /**
     * @return Preload config.
     */
    public VisorCacheRebalanceConfiguration getRebalanceConfiguration() {
        return rebalanceCfg;
    }

    /**
     * @return Eviction config.
     */
    public VisorCacheEvictionConfiguration getEvictionConfiguration() {
        return evictCfg;
    }

    /**
     * @return Store config
     */
    public VisorCacheStoreConfiguration getStoreConfiguration() {
        return storeCfg;
    }

    /**
     * @return Collection of query entities.
     */
    public List<VisorQueryEntity> getQueryEntities() {
        return qryEntities;
    }

    /**
     * @return Collection of query entities.
     */
    public VisorQueryConfiguration getQueryConfiguration() {
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
    @Override public byte getProtocolVersion() {
        return V2;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, name);
        U.writeString(out, grpName);
        U.writeEnum(out, mode);
        U.writeEnum(out, atomicityMode);
        out.writeBoolean(eagerTtl);
        U.writeEnum(out, writeSynchronizationMode);
        out.writeBoolean(invalidate);
        out.writeInt(maxConcurrentAsyncOps);
        U.writeString(out, interceptor);
        out.writeLong(dfltLockTimeout);
        out.writeObject(affinityCfg);
        out.writeObject(rebalanceCfg);
        out.writeObject(evictCfg);
        out.writeObject(nearCfg);
        out.writeObject(storeCfg);
        U.writeCollection(out, qryEntities);
        U.writeCollection(out, jdbcTypes);
        out.writeBoolean(statisticsEnabled);
        out.writeBoolean(mgmtEnabled);
        U.writeString(out, ldrFactory);
        U.writeString(out, writerFactory);
        U.writeString(out, expiryPlcFactory);
        out.writeObject(qryCfg);
        out.writeBoolean(sys);
        out.writeBoolean(storeKeepBinary);
        out.writeBoolean(onheapCache);
        U.writeEnum(out, partLossPlc);
        out.writeInt(qryParallelism);
        out.writeBoolean(cpOnRead);
        U.writeString(out, evictFilter);
        U.writeString(out, lsnrConfigurations);
        out.writeBoolean(loadPrevVal);
        U.writeString(out, dataRegName);
        out.writeInt(sqlIdxMaxInlineSize);
        U.writeString(out, nodeFilter);
        out.writeInt(qryDetailMetricsSz);
        out.writeBoolean(readFromBackup);
        U.writeString(out, tmLookupClsName);
        U.writeString(out, topValidator);
        U.writeIgniteUuid(out, dynamicDeploymentId);

        // V2
        U.writeEnum(out, diskPageCompression);
        out.writeObject(diskPageCompressionLevel);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        name = U.readString(in);
        grpName = U.readString(in);
        mode = CacheMode.fromOrdinal(in.readByte());
        atomicityMode = CacheAtomicityMode.fromOrdinal(in.readByte());
        eagerTtl = in.readBoolean();
        writeSynchronizationMode = CacheWriteSynchronizationMode.fromOrdinal(in.readByte());
        invalidate = in.readBoolean();
        maxConcurrentAsyncOps = in.readInt();
        interceptor = U.readString(in);
        dfltLockTimeout = in.readLong();
        affinityCfg = (VisorCacheAffinityConfiguration)in.readObject();
        rebalanceCfg = (VisorCacheRebalanceConfiguration)in.readObject();
        evictCfg = (VisorCacheEvictionConfiguration)in.readObject();
        nearCfg = (VisorCacheNearConfiguration)in.readObject();
        storeCfg = (VisorCacheStoreConfiguration)in.readObject();
        qryEntities = U.readList(in);
        jdbcTypes = U.readList(in);
        statisticsEnabled = in.readBoolean();
        mgmtEnabled = in.readBoolean();
        ldrFactory = U.readString(in);
        writerFactory = U.readString(in);
        expiryPlcFactory = U.readString(in);
        qryCfg = (VisorQueryConfiguration)in.readObject();
        sys = in.readBoolean();
        storeKeepBinary = in.readBoolean();
        onheapCache = in.readBoolean();
        partLossPlc = PartitionLossPolicy.fromOrdinal(in.readByte());
        qryParallelism = in.readInt();
        cpOnRead = in.readBoolean();
        evictFilter = U.readString(in);
        lsnrConfigurations = U.readString(in);
        loadPrevVal = in.readBoolean();
        dataRegName = U.readString(in);
        sqlIdxMaxInlineSize = in.readInt();
        nodeFilter = U.readString(in);
        qryDetailMetricsSz = in.readInt();
        readFromBackup = in.readBoolean();
        tmLookupClsName = U.readString(in);
        topValidator = U.readString(in);
        dynamicDeploymentId = U.readIgniteUuid(in);

        if (protoVer > V1) {
            diskPageCompression = DiskPageCompression.fromOrdinal(in.readByte());
            diskPageCompressionLevel = (Integer) in.readObject();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheConfiguration.class, this);
    }
}
