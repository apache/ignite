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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.apache.ignite.internal.visor.query.VisorQueryConfiguration;
import org.apache.ignite.internal.visor.query.VisorQueryEntity;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;

/**
 * Data transfer object for cache configuration properties.
 */
public class VisorCacheConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String name;

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

    /** Start size. */
    private int startSize;

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
     */
    public VisorCacheConfiguration(IgniteEx ignite, CacheConfiguration ccfg) {
        name = ccfg.getName();
        mode = ccfg.getCacheMode();
        atomicityMode = ccfg.getAtomicityMode();
        eagerTtl = ccfg.isEagerTtl();
        writeSynchronizationMode = ccfg.getWriteSynchronizationMode();
        invalidate = ccfg.isInvalidate();
        startSize = ccfg.getStartSize();
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
    }

    /**
     * @return Cache name.
     */
    @Nullable public String getName() {
        return name;
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
     * @return Start size.
     */
    public int getStartSize() {
        return startSize;
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
     * @return Default lock acquisition timeout.
     */
    public long getDfltLockTimeout() {
        return dfltLockTimeout;
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

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, name);
        U.writeEnum(out, mode);
        U.writeEnum(out, atomicityMode);
        out.writeBoolean(eagerTtl);
        U.writeEnum(out, writeSynchronizationMode);
        out.writeBoolean(invalidate);
        out.writeInt(startSize);
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
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        name = U.readString(in);
        mode = CacheMode.fromOrdinal(in.readByte());
        atomicityMode = CacheAtomicityMode.fromOrdinal(in.readByte());
        eagerTtl = in.readBoolean();
        writeSynchronizationMode = CacheWriteSynchronizationMode.fromOrdinal(in.readByte());
        invalidate = in.readBoolean();
        startSize = in.readInt();
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
        onheapCache = in.readBoolean();
        partLossPlc = PartitionLossPolicy.fromOrdinal(in.readByte());
        qryParallelism = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheConfiguration.class, this);
    }
}
