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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.InstanceNotFoundException;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.DataStorageMetrics;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.file.MappedFileMemoryProvider;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.persistence.evict.FairFifoPageEvictionTracker;
import org.apache.ignite.internal.processors.cache.persistence.evict.NoOpPageEvictionTracker;
import org.apache.ignite.internal.processors.cache.persistence.evict.PageEvictionTracker;
import org.apache.ignite.internal.processors.cache.persistence.evict.Random2LruPageEvictionTracker;
import org.apache.ignite.internal.processors.cache.persistence.evict.RandomLruPageEvictionTracker;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.freelist.CacheFreeListImpl;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.mxbean.DataRegionMetricsMXBean;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_REUSE_MEMORY_ON_DEACTIVATE;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_MAX_SIZE;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_HISTORY_SIZE;

/**
 *
 */
public class IgniteCacheDatabaseSharedManager extends GridCacheSharedManagerAdapter
    implements IgniteChangeGlobalStateSupport, CheckpointLockStateChecker {
    /** DataRegionConfiguration name reserved for internal caches. */
    public static final String SYSTEM_DATA_REGION_NAME = "sysMemPlc";

    /** Minimum size of memory chunk */
    private static final long MIN_PAGE_MEMORY_SIZE = 10L * 1024 * 1024;

    /** Maximum initial size on 32-bit JVM */
    private static final long MAX_PAGE_MEMORY_INIT_SIZE_32_BIT = 2L * 1024 * 1024 * 1024;

    /** {@code True} to reuse memory on deactive. */
    private final boolean reuseMemory = IgniteSystemProperties.getBoolean(IGNITE_REUSE_MEMORY_ON_DEACTIVATE);

    /** */
    protected volatile Map<String, DataRegion> dataRegionMap;

    /** */
    private volatile boolean dataRegionsInitialized;

    /** */
    protected Map<String, DataRegionMetrics> memMetricsMap;

    /** */
    protected DataRegion dfltDataRegion;

    /** */
    protected Map<String, CacheFreeListImpl> freeListMap;

    /** */
    private CacheFreeListImpl dfltFreeList;

    /** Page size from memory configuration, may be set only for fake(standalone) IgniteCacheDataBaseSharedManager */
    private int pageSize;

    /** First eviction was warned flag. */
    private volatile boolean firstEvictWarn;

    /** Stores memory providers eligible for reuse. */
    private Map<String, DirectMemoryProvider> memProviderMap;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        if (cctx.kernalContext().clientNode() && cctx.kernalContext().config().getDataStorageConfiguration() == null)
            return;

        DataStorageConfiguration memCfg = cctx.kernalContext().config().getDataStorageConfiguration();

        assert memCfg != null;

        validateConfiguration(memCfg);

        pageSize = memCfg.getPageSize();

        initDataRegions(memCfg);
    }

    /**
     * Registers MBeans for all DataRegionMetrics configured in this instance.
     */
    private void registerMetricsMBeans() {
        if(U.IGNITE_MBEANS_DISABLED)
            return;

        IgniteConfiguration cfg = cctx.gridConfig();

        for (DataRegionMetrics memMetrics : memMetricsMap.values()) {
            DataRegionConfiguration memPlcCfg = dataRegionMap.get(memMetrics.getName()).config();

            registerMetricsMBean((DataRegionMetricsImpl)memMetrics, memPlcCfg, cfg);
        }
    }

    /**
     * @param memMetrics Memory metrics.
     * @param dataRegionCfg Data region configuration.
     * @param cfg Ignite configuration.
     */
    private void registerMetricsMBean(
        DataRegionMetricsImpl memMetrics,
        DataRegionConfiguration dataRegionCfg,
        IgniteConfiguration cfg
    ) {
        assert !U.IGNITE_MBEANS_DISABLED;

        try {
            U.registerMBean(
                cfg.getMBeanServer(),
                cfg.getIgniteInstanceName(),
                "DataRegionMetrics",
                dataRegionCfg.getName(),
                new DataRegionMetricsMXBeanImpl(memMetrics, dataRegionCfg),
                DataRegionMetricsMXBean.class);
        }
        catch (Throwable e) {
            U.error(log, "Failed to register MBean for DataRegionMetrics with name: '" + memMetrics.getName() + "'", e);
        }
    }

    /**
     * @param dbCfg Database config.
     * @throws IgniteCheckedException If failed.
     */
    protected void initPageMemoryDataStructures(DataStorageConfiguration dbCfg) throws IgniteCheckedException {
        freeListMap = U.newHashMap(dataRegionMap.size());

        String dfltMemPlcName = dbCfg.getDefaultDataRegionConfiguration().getName();

        for (DataRegion memPlc : dataRegionMap.values()) {
            DataRegionConfiguration memPlcCfg = memPlc.config();

            DataRegionMetricsImpl memMetrics = (DataRegionMetricsImpl) memMetricsMap.get(memPlcCfg.getName());

            boolean persistenceEnabled = memPlcCfg.isPersistenceEnabled();

            CacheFreeListImpl freeList = new CacheFreeListImpl(0,
                    cctx.igniteInstanceName(),
                    memMetrics,
                    memPlc,
                    null,
                    persistenceEnabled ? cctx.wal() : null,
                    0L,
                    true);

            freeListMap.put(memPlcCfg.getName(), freeList);
        }

        dfltFreeList = freeListMap.get(dfltMemPlcName);
    }

    /**
     * @return Size of page used for PageMemory regions.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     *
     */
    private void startMemoryPolicies() {
        for (DataRegion memPlc : dataRegionMap.values()) {
            memPlc.pageMemory().start();

            memPlc.evictionTracker().start();
        }
    }

    /**
     * @param memCfg Database config.
     * @throws IgniteCheckedException If failed to initialize swap path.
     */
    protected void initDataRegions(DataStorageConfiguration memCfg) throws IgniteCheckedException {
        if (dataRegionsInitialized)
            return;

        initDataRegions0(memCfg);

        dataRegionsInitialized = true;
    }

    /**
     * @param memCfg Database config.
     * @throws IgniteCheckedException If failed to initialize swap path.
     */
    protected void initDataRegions0(DataStorageConfiguration memCfg) throws IgniteCheckedException {
        DataRegionConfiguration[] dataRegionCfgs = memCfg.getDataRegionConfigurations();

        int dataRegions = dataRegionCfgs == null ? 0 : dataRegionCfgs.length;

        dataRegionMap = U.newHashMap(3 + dataRegions);
        memMetricsMap = U.newHashMap(3 + dataRegions);
        memProviderMap = reuseMemory ? U.newHashMap(3 + dataRegions) : null;

        if (dataRegionCfgs != null) {
            for (DataRegionConfiguration dataRegionCfg : dataRegionCfgs)
                addDataRegion(memCfg, dataRegionCfg, dataRegionCfg.isPersistenceEnabled());
        }

        addDataRegion(
            memCfg,
            memCfg.getDefaultDataRegionConfiguration(),
            memCfg.getDefaultDataRegionConfiguration().isPersistenceEnabled()
        );

        addDataRegion(
            memCfg,
            createSystemDataRegion(
                memCfg.getSystemRegionInitialSize(),
                memCfg.getSystemRegionMaxSize(),
                CU.isPersistenceEnabled(memCfg)
            ),
            CU.isPersistenceEnabled(memCfg)
        );

        for (DatabaseLifecycleListener lsnr : getDatabaseListeners(cctx.kernalContext()))
            lsnr.onInitDataRegions(this);
    }

    /**
     * @param kctx Kernal context.
     * @return Database lifecycle listeners.
     */
    protected List<DatabaseLifecycleListener> getDatabaseListeners(GridKernalContext kctx) {
        return kctx.internalSubscriptionProcessor().getDatabaseListeners();
    }

    /**
     * @param dataStorageCfg Database config.
     * @param dataRegionCfg Data region config.
     * @throws IgniteCheckedException If failed to initialize swap path.
     */
    public void addDataRegion(
        DataStorageConfiguration dataStorageCfg,
        DataRegionConfiguration dataRegionCfg,
        boolean trackable
    ) throws IgniteCheckedException {
        String dataRegionName = dataRegionCfg.getName();

        String dfltMemPlcName = dataStorageCfg.getDefaultDataRegionConfiguration().getName();

        if (dfltMemPlcName == null)
            dfltMemPlcName = DFLT_DATA_REG_DEFAULT_NAME;

        DataRegionMetricsImpl memMetrics = new DataRegionMetricsImpl(dataRegionCfg, freeSpaceProvider(dataRegionCfg));

        DataRegion memPlc = initMemory(dataStorageCfg, dataRegionCfg, memMetrics, trackable);

        dataRegionMap.put(dataRegionName, memPlc);

        memMetricsMap.put(dataRegionName, memMetrics);

        if (dataRegionName.equals(dfltMemPlcName))
            dfltDataRegion = memPlc;
        else if (dataRegionName.equals(DFLT_DATA_REG_DEFAULT_NAME))
            U.warn(log, "Data Region with name 'default' isn't used as a default. " +
                    "Please check Memory Policies configuration.");
    }

    /**
     * Closure that can be used to compute fill factor for provided data region.
     *
     * @param dataRegCfg Data region configuration.
     * @return Closure.
     */
    protected IgniteOutClosure<Long> freeSpaceProvider(final DataRegionConfiguration dataRegCfg) {
        final String dataRegName = dataRegCfg.getName();

        return new IgniteOutClosure<Long>() {
            private CacheFreeListImpl freeList;

            @Override public Long apply() {
                if (freeList == null) {
                    CacheFreeListImpl freeList0 = freeListMap.get(dataRegName);

                    if (freeList0 == null)
                        return 0L;

                    freeList = freeList0;
                }

                return freeList.freeSpace();
            }
        };
    }

    /**
     * @param memPlcsCfgs User-defined data region configurations.
     */
    private boolean hasCustomDefaultDataRegion(DataRegionConfiguration[] memPlcsCfgs) {
        for (DataRegionConfiguration memPlcsCfg : memPlcsCfgs) {
            if (DFLT_DATA_REG_DEFAULT_NAME.equals(memPlcsCfg.getName()))
                return true;
        }

        return false;
    }

    /**
     * @param sysCacheInitSize Initial size of PageMemory to be created for system cache.
     * @param sysCacheMaxSize Maximum size of PageMemory to be created for system cache.
     * @param persistenceEnabled Persistence enabled flag.
     *
     * @return {@link DataRegionConfiguration configuration} of DataRegion for system cache.
     */
    private DataRegionConfiguration createSystemDataRegion(
        long sysCacheInitSize,
        long sysCacheMaxSize,
        boolean persistenceEnabled
    ) {
        DataRegionConfiguration res = new DataRegionConfiguration();

        res.setName(SYSTEM_DATA_REGION_NAME);
        res.setInitialSize(sysCacheInitSize);
        res.setMaxSize(sysCacheMaxSize);
        res.setPersistenceEnabled(persistenceEnabled);

        return res;
    }

    /**
     * @param memCfg configuration to validate.
     */
    private void validateConfiguration(DataStorageConfiguration memCfg) throws IgniteCheckedException {
        checkPageSize(memCfg);

        DataRegionConfiguration[] regCfgs = memCfg.getDataRegionConfigurations();

        Set<String> regNames = (regCfgs != null) ? U.<String>newHashSet(regCfgs.length) : new HashSet<String>(0);

        checkSystemDataRegionSizeConfiguration(
            memCfg.getSystemRegionInitialSize(),
            memCfg.getSystemRegionMaxSize()
        );

        if (regCfgs != null) {
            for (DataRegionConfiguration regCfg : regCfgs)
                checkDataRegionConfiguration(memCfg, regNames, regCfg);
        }

        checkDataRegionConfiguration(memCfg, regNames, memCfg.getDefaultDataRegionConfiguration());

        checkWalArchiveSizeConfiguration(memCfg);
    }

    /**
     * Check wal archive size configuration for correctness.
     *
     * @param memCfg durable memory configuration for an Apache Ignite node.
     */
    private void checkWalArchiveSizeConfiguration(DataStorageConfiguration memCfg) throws IgniteCheckedException {
        if (memCfg.getWalHistorySize() == DFLT_WAL_HISTORY_SIZE || memCfg.getWalHistorySize() == Integer.MAX_VALUE)
            LT.warn(log, "DataRegionConfiguration.maxWalArchiveSize instead DataRegionConfiguration.walHistorySize " +
            "would be used for removing old archive wal files");
        else if(memCfg.getMaxWalArchiveSize() == DFLT_WAL_ARCHIVE_MAX_SIZE)
            LT.warn(log, "walHistorySize was deprecated. maxWalArchiveSize should be used instead");
        else
            throw new IgniteCheckedException("Should be used only one of wal history size or max wal archive size." +
                "(use DataRegionConfiguration.maxWalArchiveSize because DataRegionConfiguration.walHistorySize was deprecated)"
            );

        if(memCfg.getMaxWalArchiveSize() < memCfg.getWalSegmentSize())
            throw new IgniteCheckedException(
                "DataRegionConfiguration.maxWalArchiveSize should be greater than DataRegionConfiguration.walSegmentSize"
            );
    }

    /**
     * @param memCfg Mem config.
     * @param regNames Region names.
     * @param regCfg Reg config.
     */
    private void checkDataRegionConfiguration(DataStorageConfiguration memCfg, Set<String> regNames,
        DataRegionConfiguration regCfg) throws IgniteCheckedException {
        assert regCfg != null;

        checkDataRegionName(regCfg.getName(), regNames);

        checkDataRegionSize(regCfg);

        checkMetricsProperties(regCfg);

        checkRegionEvictionProperties(regCfg, memCfg);

        checkRegionMemoryStorageType(regCfg);
    }

    /**
     * @param memCfg Memory config.
     */
    protected void checkPageSize(DataStorageConfiguration memCfg) {
        if (memCfg.getPageSize() == 0)
            memCfg.setPageSize(DFLT_PAGE_SIZE);
    }

    /**
     * @param regCfg data region config.
     *
     * @throws IgniteCheckedException if validation of memory metrics properties fails.
     */
    private static void checkMetricsProperties(DataRegionConfiguration regCfg) throws IgniteCheckedException {
        if (regCfg.getMetricsRateTimeInterval() <= 0)
            throw new IgniteCheckedException("Rate time interval must be greater than zero " +
                "(use DataRegionConfiguration.rateTimeInterval property to adjust the interval) " +
                "[name=" + regCfg.getName() +
                ", rateTimeInterval=" + regCfg.getMetricsRateTimeInterval() + "]"
            );
        if (regCfg.getMetricsSubIntervalCount() <= 0)
            throw new IgniteCheckedException("Sub intervals must be greater than zero " +
                "(use DataRegionConfiguration.subIntervals property to adjust the sub intervals) " +
                "[name=" + regCfg.getName() +
                ", subIntervals=" + regCfg.getMetricsSubIntervalCount() + "]"
            );

        if (regCfg.getMetricsRateTimeInterval() < 1_000)
            throw new IgniteCheckedException("Rate time interval must be longer that 1 second (1_000 milliseconds) " +
                "(use DataRegionConfiguration.rateTimeInterval property to adjust the interval) " +
                "[name=" + regCfg.getName() +
                ", rateTimeInterval=" + regCfg.getMetricsRateTimeInterval() + "]");
    }

    /**
     * @param sysCacheInitSize System cache initial size.
     * @param sysCacheMaxSize System cache max size.
     *
     * @throws IgniteCheckedException In case of validation violation.
     */
    private static void checkSystemDataRegionSizeConfiguration(
        long sysCacheInitSize,
        long sysCacheMaxSize
    ) throws IgniteCheckedException {
        if (sysCacheInitSize < MIN_PAGE_MEMORY_SIZE)
            throw new IgniteCheckedException("Initial size for system cache must have size more than 10MB (use " +
                "DataStorageConfiguration.systemCacheInitialSize property to set correct size in bytes) " +
                "[size=" + U.readableSize(sysCacheInitSize, true) + ']'
            );

        if (U.jvm32Bit() && sysCacheInitSize > MAX_PAGE_MEMORY_INIT_SIZE_32_BIT)
            throw new IgniteCheckedException("Initial size for system cache exceeds 2GB on 32-bit JVM (use " +
                "DataRegionConfiguration.systemCacheInitialSize property to set correct size in bytes " +
                "or use 64-bit JVM) [size=" + U.readableSize(sysCacheInitSize, true) + ']'
            );

        if (sysCacheMaxSize < sysCacheInitSize)
            throw new IgniteCheckedException("MaxSize of system cache must not be smaller than " +
                "initialSize [initSize=" + U.readableSize(sysCacheInitSize, true) +
                ", maxSize=" + U.readableSize(sysCacheMaxSize, true) + "]. " +
                "Use DataStorageConfiguration.systemCacheInitialSize/DataStorageConfiguration.systemCacheMaxSize " +
                "properties to set correct sizes in bytes."
            );
    }

    /**
     * @param regCfg DataRegionConfiguration to validate.
     * @throws IgniteCheckedException If config is invalid.
     */
    private void checkDataRegionSize(DataRegionConfiguration regCfg) throws IgniteCheckedException {
        if (regCfg.getInitialSize() < MIN_PAGE_MEMORY_SIZE || regCfg.getMaxSize() < MIN_PAGE_MEMORY_SIZE)
            throw new IgniteCheckedException("DataRegion must have size more than 10MB (use " +
                "DataRegionConfiguration.initialSize and .maxSize properties to set correct size in bytes) " +
                "[name=" + regCfg.getName() + ", initialSize=" + U.readableSize(regCfg.getInitialSize(), true) +
                ", maxSize=" + U.readableSize(regCfg.getMaxSize(), true) + "]"
            );

        if (regCfg.getMaxSize() < regCfg.getInitialSize()) {
            if (regCfg.getInitialSize() != Math.min(DataStorageConfiguration.DFLT_DATA_REGION_MAX_SIZE,
                DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)) {
                throw new IgniteCheckedException("DataRegion maxSize must not be smaller than initialSize" +
                    "[name=" + regCfg.getName() + ", initialSize=" + U.readableSize(regCfg.getInitialSize(), true) +
                    ", maxSize=" + U.readableSize(regCfg.getMaxSize(), true) + "]");
            }

            regCfg.setInitialSize(regCfg.getMaxSize());

            LT.warn(log, "DataRegion maxSize=" + U.readableSize(regCfg.getMaxSize(), true) +
                " is smaller than defaultInitialSize=" +
                U.readableSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE, true) +
                ", setting initialSize to " + U.readableSize(regCfg.getMaxSize(), true));
        }

        if (U.jvm32Bit() && regCfg.getInitialSize() > MAX_PAGE_MEMORY_INIT_SIZE_32_BIT)
            throw new IgniteCheckedException("DataRegion initialSize exceeds 2GB on 32-bit JVM (use " +
                "DataRegionConfiguration.initialSize property to set correct size in bytes or use 64-bit JVM) " +
                "[name=" + regCfg.getName() +
                ", size=" + U.readableSize(regCfg.getInitialSize(), true) + "]");
    }

    /**
     * @param regCfg DataRegionConfiguration to validate.
     * @throws IgniteCheckedException If config is invalid.
     */
    private void checkRegionMemoryStorageType(DataRegionConfiguration regCfg) throws IgniteCheckedException {
        if (regCfg.isPersistenceEnabled() && regCfg.getSwapPath() != null)
            throw new IgniteCheckedException("DataRegionConfiguration must not have both persistence " +
                "storage and swap space enabled at the same time (Use DataRegionConfiguration.setSwapPath(null)  " +
                "to disable the swap space usage or DataRegionConfiguration.setPersistenceEnabled(false) " +
                "to disable the persistence) [name=" + regCfg.getName() + ", swapPath=" + regCfg.getSwapPath() +
                ", persistenceEnabled=" + regCfg.isPersistenceEnabled() + "]"
            );
    }

    /**
     * @param regCfg DataRegionConfiguration to validate.
     * @param dbCfg Memory configuration.
     * @throws IgniteCheckedException If config is invalid.
     */
    protected void checkRegionEvictionProperties(DataRegionConfiguration regCfg, DataStorageConfiguration dbCfg)
        throws IgniteCheckedException {
        if (regCfg.getPageEvictionMode() == DataPageEvictionMode.DISABLED)
            return;

        if (regCfg.getEvictionThreshold() < 0.5 || regCfg.getEvictionThreshold() > 0.999) {
            throw new IgniteCheckedException("Page eviction threshold must be between 0.5 and 0.999: " +
                regCfg.getName());
        }

        if (regCfg.getEmptyPagesPoolSize() <= 10)
            throw new IgniteCheckedException("Evicted pages pool size should be greater than 10: " + regCfg.getName());

        long maxPoolSize = regCfg.getMaxSize() / dbCfg.getPageSize() / 10;

        if (regCfg.getEmptyPagesPoolSize() >= maxPoolSize) {
            throw new IgniteCheckedException("Evicted pages pool size should be lesser than " + maxPoolSize +
                ": " + regCfg.getName());
        }
    }

    /**
     * @param regName DataRegion name to validate.
     * @param observedNames Names of MemoryPolicies observed before.
     * @throws IgniteCheckedException If config is invalid.
     */
    private static void checkDataRegionName(String regName, Collection<String> observedNames)
        throws IgniteCheckedException {
        if (regName == null || regName.isEmpty())
            throw new IgniteCheckedException("User-defined DataRegionConfiguration must have non-null and " +
                "non-empty name.");

        if (observedNames.contains(regName))
            throw new IgniteCheckedException("Two MemoryPolicies have the same name: " + regName);

        if (SYSTEM_DATA_REGION_NAME.equals(regName))
            throw new IgniteCheckedException("'" + SYSTEM_DATA_REGION_NAME + "' policy name is reserved for internal use.");

        observedNames.add(regName);
    }

    /**
     * @param log Logger.
     */
    public void dumpStatistics(IgniteLogger log) {
        if (freeListMap != null) {
            for (CacheFreeListImpl freeList : freeListMap.values())
                freeList.dumpStatistics(log);
        }
    }

    /**
     * @return collection of all configured {@link DataRegion policies}.
     */
    public Collection<DataRegion> dataRegions() {
        return dataRegionMap != null ? dataRegionMap.values() : null;
    }

    /**
     * @return DataRegionMetrics for all MemoryPolicies configured in Ignite instance.
     */
    public Collection<DataRegionMetrics> memoryMetrics() {
        if (!F.isEmpty(memMetricsMap)) {
            // Intentionally return a collection copy to make it explicitly serializable.
            Collection<DataRegionMetrics> res = new ArrayList<>(memMetricsMap.size());

            for (DataRegionMetrics metrics : memMetricsMap.values())
                res.add(new DataRegionMetricsSnapshot(metrics));

            return res;
        }
        else
            return Collections.emptyList();
    }

    /**
     * @return DataStorageMetrics if persistence is enabled or {@code null} otherwise.
     */
    public DataStorageMetrics persistentStoreMetrics() {
        return null;
    }

    /**
     * @param cachesToStart Started caches.
     * @throws IgniteCheckedException If failed.
     */
    public void readCheckpointAndRestoreMemory(List<DynamicCacheDescriptor> cachesToStart) throws IgniteCheckedException {
        // No-op.
    }

    /**
     * @param memPlcName Name of {@link DataRegion} to obtain {@link DataRegionMetrics} for.
     * @return {@link DataRegionMetrics} snapshot for specified {@link DataRegion} or {@code null} if
     * no {@link DataRegion} is configured for specified name.
     */
    @Nullable public DataRegionMetrics memoryMetrics(String memPlcName) {
        if (!F.isEmpty(memMetricsMap)) {
            DataRegionMetrics memMetrics = memMetricsMap.get(memPlcName);

            return memMetrics == null ? null : new DataRegionMetricsSnapshot(memMetrics);
        }
        else
            return null;
    }

    /**
     * @param memPlcName data region name.
     * @return {@link DataRegion} instance associated with a given {@link DataRegionConfiguration}.
     * @throws IgniteCheckedException in case of request for unknown DataRegion.
     */
    public DataRegion dataRegion(String memPlcName) throws IgniteCheckedException {
        if (memPlcName == null)
            return dfltDataRegion;

        if (dataRegionMap == null)
            return null;

        DataRegion plc;

        if ((plc = dataRegionMap.get(memPlcName)) == null)
            throw new IgniteCheckedException("Requested DataRegion is not configured: " + memPlcName);

        return plc;
    }

    /**
     * @param memPlcName DataRegionConfiguration name.
     * @return {@link FreeList} instance associated with a given {@link DataRegionConfiguration}.
     */
    public FreeList freeList(String memPlcName) {
        if (memPlcName == null)
            return dfltFreeList;

        return freeListMap != null ? freeListMap.get(memPlcName) : null;
    }

    /**
     * @param memPlcName DataRegionConfiguration name.
     * @return {@link ReuseList} instance associated with a given {@link DataRegionConfiguration}.
     */
    public ReuseList reuseList(String memPlcName) {
        if (memPlcName == null)
            return dfltFreeList;

        return freeListMap != null ? freeListMap.get(memPlcName) : null;
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        onDeActivate(true);
    }

    /**
     * Unregister MBean.
     * @param name Name of mbean.
     */
    private void unregisterMBean(String name) {
        if(U.IGNITE_MBEANS_DISABLED)
            return;

        IgniteConfiguration cfg = cctx.gridConfig();

        try {
            cfg.getMBeanServer().unregisterMBean(
                U.makeMBeanName(
                    cfg.getIgniteInstanceName(),
                    "DataRegionMetrics", name
                    ));
        }
        catch (InstanceNotFoundException ignored) {
            // We tried to unregister a non-existing MBean, not a big deal.
        }
        catch (Throwable e) {
            U.error(log, "Failed to unregister MBean for memory metrics: " +
                name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean checkpointLockIsHeldByThread() {
        return true;
    }

    /**
     * No-op for non-persistent storage.
     */
    public void checkpointReadLock() {
        // No-op.
    }

    /**
     * No-op for non-persistent storage.
     */
    public void checkpointReadUnlock() {
        // No-op.
    }

    /**
     * No-op for non-persistent storage.
     */
    public void cleanupCheckpointDirectory() throws IgniteCheckedException {
        // No-op.
    }

    /**
     * No-op for non-persistent storage.
     */
    public void cleanupTempCheckpointDirectory() throws IgniteCheckedException{
        // No-op.
    }

    /**
     *
     */
    @Nullable public IgniteInternalFuture wakeupForCheckpoint(String reason) {
        return null;
    }

    /**
     * @return Last checkpoint mark WAL pointer.
     */
    public WALPointer lastCheckpointMarkWalPointer() {
        return null;
    }

    /**
     * Allows to wait checkpoint finished.
     *
     * @param reason Reason.
     */
    @Nullable public CheckpointFuture forceCheckpoint(String reason) {
        return null;
    }

    /**
     * Waits until current state is checkpointed.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void waitForCheckpoint(String reason) throws IgniteCheckedException {
        // No-op
    }

    /**
     * @param discoEvt Before exchange for the given discovery event.
     *
     * @return {@code True} if partitions have been restored from persistent storage.
     */
    public boolean beforeExchange(GridDhtPartitionsExchangeFuture discoEvt) throws IgniteCheckedException {
        return false;
    }

    /**
     * Called when all partitions have been fully restored and pre-created on node start.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void onStateRestored() throws IgniteCheckedException {
        // No-op.
    }

        /**
         * @param fut Partition exchange future.
         */
    public void rebuildIndexesIfNeeded(GridDhtPartitionsExchangeFuture fut) {
        // No-op.
    }

    /**
     * Needed action before any cache will stop
     */
    public void prepareCachesStop() {
        // No-op.
    }

    /**
     * @param stoppedGrps A collection of tuples (cache group, destroy flag).
     */
    public void onCacheGroupsStopped(Collection<IgniteBiTuple<CacheGroupContext, Boolean>> stoppedGrps) {
        // No-op.
    }

    /**
     * @return Future that will be completed when indexes for given cache are restored.
     */
    @Nullable public IgniteInternalFuture indexRebuildFuture(int cacheId) {
        return null;
    }

    /**
     * Reserve update history for exchange.
     *
     * @return Reserved update counters per cache and partition.
     */
    public Map<Integer, Map<Integer, Long>> reserveHistoryForExchange() {
        return Collections.emptyMap();
    }

    /**
     * Release reserved update history.
     */
    public void releaseHistoryForExchange() {
        // No-op
    }

    /**
     * Reserve update history for preloading.
     * @param grpId Cache group ID.
     * @param partId Partition Id.
     * @param cntr Update counter.
     * @return True if successfully reserved.
     */
    public boolean reserveHistoryForPreloading(int grpId, int partId, long cntr) {
        return false;
    }

    /**
     * Release reserved update history.
     */
    public void releaseHistoryForPreloading() {
        // No-op
    }

    /**
     * See {@link GridCacheMapEntry#ensureFreeSpace()}
     *
     * @param memPlc data region.
     */
    public void ensureFreeSpace(DataRegion memPlc) throws IgniteCheckedException {
        if (memPlc == null)
            return;

        DataRegionConfiguration plcCfg = memPlc.config();

        if (plcCfg.getPageEvictionMode() == DataPageEvictionMode.DISABLED || plcCfg.isPersistenceEnabled())
            return;

        long memorySize = plcCfg.getMaxSize();

        PageMemory pageMem = memPlc.pageMemory();

        int sysPageSize = pageMem.systemPageSize();

        CacheFreeListImpl freeListImpl = freeListMap.get(plcCfg.getName());

        for (;;) {
            long allocatedPagesCnt = pageMem.loadedPages();

            int emptyDataPagesCnt = freeListImpl.emptyDataPages();

            boolean shouldEvict = allocatedPagesCnt > (memorySize / sysPageSize * plcCfg.getEvictionThreshold()) &&
                emptyDataPagesCnt < plcCfg.getEmptyPagesPoolSize();

            if (shouldEvict) {
                warnFirstEvict(plcCfg);

                memPlc.evictionTracker().evictDataPage();

                memPlc.memoryMetrics().updateEvictionRate();
            } else
                break;
        }
    }

    /**
     * @param memCfg memory configuration with common parameters.
     * @param plcCfg data region with PageMemory specific parameters.
     * @param memMetrics {@link DataRegionMetrics} object to collect memory usage metrics.
     * @return data region instance.
     *
     * @throws IgniteCheckedException If failed to initialize swap path.
     */
    private DataRegion initMemory(
        DataStorageConfiguration memCfg,
        DataRegionConfiguration plcCfg,
        DataRegionMetricsImpl memMetrics,
        boolean trackable
    ) throws IgniteCheckedException {
        PageMemory pageMem = createPageMemory(createOrReuseMemoryProvider(plcCfg), memCfg, plcCfg, memMetrics, trackable);

        return new DataRegion(pageMem, plcCfg, memMetrics, createPageEvictionTracker(plcCfg, pageMem));
    }

    /**
     * @param plcCfg Policy config.
     * @return DirectMemoryProvider provider.
     */
    private DirectMemoryProvider createOrReuseMemoryProvider(DataRegionConfiguration plcCfg)
        throws IgniteCheckedException {
        if (!supportsMemoryReuse(plcCfg))
            return createMemoryProvider(plcCfg);

        DirectMemoryProvider memProvider = memProviderMap.get(plcCfg.getName());

        if (memProvider == null)
            memProviderMap.put(plcCfg.getName(), (memProvider = createMemoryProvider(plcCfg)));

        return memProvider;
    }

    /**
     * @param plcCfg Policy config.
     *
     * @return {@code True} if policy supports memory reuse.
     */
    private boolean supportsMemoryReuse(DataRegionConfiguration plcCfg) {
        return reuseMemory && plcCfg.getSwapPath() == null;
    }

    /**
     * @param plcCfg Policy config.
     * @return DirectMemoryProvider provider.
     */
    private DirectMemoryProvider createMemoryProvider(DataRegionConfiguration plcCfg) throws IgniteCheckedException {
        File allocPath = buildAllocPath(plcCfg);

        return allocPath == null ?
            new UnsafeMemoryProvider(log) :
            new MappedFileMemoryProvider(
                log,
                allocPath);
    }

    /**
     * @param plc data region Configuration.
     * @param pageMem Page memory.
     */
    protected PageEvictionTracker createPageEvictionTracker(DataRegionConfiguration plc, PageMemory pageMem) {
        if (plc.getPageEvictionMode() == DataPageEvictionMode.DISABLED || plc.isPersistenceEnabled())
            return new NoOpPageEvictionTracker();

        assert pageMem instanceof PageMemoryNoStoreImpl : pageMem.getClass();

        PageMemoryNoStoreImpl pageMem0 = (PageMemoryNoStoreImpl)pageMem;

        if (Boolean.getBoolean("override.fair.fifo.page.eviction.tracker"))
            return new FairFifoPageEvictionTracker(pageMem0, plc, cctx);

        switch (plc.getPageEvictionMode()) {
            case RANDOM_LRU:
                return new RandomLruPageEvictionTracker(pageMem0, plc, cctx);
            case RANDOM_2_LRU:
                return new Random2LruPageEvictionTracker(pageMem0, plc, cctx);
            default:
                return new NoOpPageEvictionTracker();
        }
    }

    /**
     * Builds allocation path for memory mapped file to be used with PageMemory.
     *
     * @param plc DataRegionConfiguration.
     *
     * @throws IgniteCheckedException If resolving swap directory fails.
     */
    @Nullable protected File buildAllocPath(DataRegionConfiguration plc) throws IgniteCheckedException {
        String path = plc.getSwapPath();

        if (path == null)
            return null;

        final PdsFolderSettings folderSettings = cctx.kernalContext().pdsFolderResolver().resolveFolders();

        final String folderName = folderSettings.isCompatible() ?
            String.valueOf(folderSettings.consistentId()).replaceAll("[:,\\.]", "_") :
            folderSettings.folderName();

        return buildPath(path, folderName);
    }

    /**
     * Creates PageMemory with given size and memory provider.
     *
     * @param memProvider Memory provider.
     * @param memCfg Memory configuartion.
     * @param memPlcCfg data region configuration.
     * @param memMetrics DataRegionMetrics to collect memory usage metrics.
     * @return PageMemory instance.
     */
    protected PageMemory createPageMemory(
        DirectMemoryProvider memProvider,
        DataStorageConfiguration memCfg,
        DataRegionConfiguration memPlcCfg,
        DataRegionMetricsImpl memMetrics,
        boolean trackable
    ) {
        memMetrics.persistenceEnabled(false);

        PageMemory pageMem = new PageMemoryNoStoreImpl(
            log,
            wrapMetricsMemoryProvider(memProvider, memMetrics),
            cctx,
            memCfg.getPageSize(),
            memPlcCfg,
            memMetrics,
            false
        );

        memMetrics.pageMemory(pageMem);

        return pageMem;
    }

    /**
     * @param memoryProvider0 Memory provider.
     * @param memMetrics Memory metrics.
     * @return Wrapped memory provider.
     */
    protected DirectMemoryProvider wrapMetricsMemoryProvider(
        final DirectMemoryProvider memoryProvider0,
        final DataRegionMetricsImpl memMetrics
    ) {
        return new DirectMemoryProvider() {
            /** */
            private final DirectMemoryProvider memProvider = memoryProvider0;

            @Override public void initialize(long[] chunkSizes) {
                memProvider.initialize(chunkSizes);
            }

            @Override public void shutdown(boolean deallocate) {
                memProvider.shutdown(deallocate);
            }

            @Override public DirectMemoryRegion nextRegion() {
                DirectMemoryRegion nextMemoryRegion = memProvider.nextRegion();

                if (nextMemoryRegion == null)
                    return null;

                memMetrics.updateOffHeapSize(nextMemoryRegion.size());

                return nextMemoryRegion;
            }
        };
    }

    /**
     * @param path Path to the working directory.
     * @param consId Consistent ID of the local node.
     * @return DB storage path.
     *
     * @throws IgniteCheckedException If resolving swap directory fails.
     */
    protected File buildPath(String path, String consId) throws IgniteCheckedException {
        String igniteHomeStr = U.getIgniteHome();

        File workDir = igniteHomeStr == null ? new File(path) : U.resolveWorkDirectory(igniteHomeStr, path, false);


        return new File(workDir, consId);
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        if (cctx.kernalContext().clientNode() && cctx.kernalContext().config().getDataStorageConfiguration() == null)
            return;

        DataStorageConfiguration memCfg = cctx.kernalContext().config().getDataStorageConfiguration();

        assert memCfg != null;

        initDataRegions(memCfg);

        registerMetricsMBeans();

        startMemoryPolicies();

        initPageMemoryDataStructures(memCfg);

        for (DatabaseLifecycleListener lsnr : getDatabaseListeners(kctx))
            lsnr.afterInitialise(this);
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        onDeActivate(!reuseMemory);
    }

    /**
     * @param shutdown Shutdown.
     */
    private void onDeActivate(boolean shutdown) {
        for (DatabaseLifecycleListener lsnr : getDatabaseListeners(cctx.kernalContext()))
            lsnr.beforeStop(this);

        if (dataRegionMap != null) {
            for (DataRegion memPlc : dataRegionMap.values()) {
                memPlc.pageMemory().stop(shutdown);

                memPlc.evictionTracker().stop();

                unregisterMBean(memPlc.memoryMetrics().getName());
            }

            dataRegionMap.clear();

            dataRegionMap = null;

            if (shutdown && memProviderMap != null) {
                memProviderMap.clear();

                memProviderMap = null;
            }

            dataRegionsInitialized = false;
        }
    }

    /**
     * @return Name of DataRegionConfiguration for internal caches.
     */
    public String systemDateRegionName() {
        return SYSTEM_DATA_REGION_NAME;
    }

    /**
     * Method for fake (standalone) context initialization. Not to be called in production code
     * @param pageSize configured page size
     */
    protected void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * @return MetaStorage
     */
    public MetaStorage metaStorage() {
        return null;
    }

    /**
     * Notifies {@link MetastorageLifecycleListener} that {@link MetaStorage} is ready for read.
     * This method is called when all processors and managers have already started and right before discovery manager.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void notifyMetaStorageSubscribersOnReadyForRead() throws IgniteCheckedException {
        // No-op.
    }

    /**
     * @param grpId Group ID.
     * @return WAL enabled flag.
     */
    public boolean walEnabled(int grpId, boolean local) {
        return false;
    }

    /**
     * Marks cache group as with disabled WAL.
     *
     * @param grpId Group id.
     * @param enabled flag.
     */
    public void walEnabled(int grpId, boolean enabled, boolean local) {
        // No-op.
    }

    /**
     * Marks last checkpoint as inapplicable for WAL rebalance for given group {@code grpId}.
     *
     * @param grpId Group id.
     */
    public void lastCheckpointInapplicableForWalRebalance(int grpId) {
        // No-op.
    }

    /**
     * Warns on first eviction.
     * @param regCfg data region configuration.
     */
    private void warnFirstEvict(DataRegionConfiguration regCfg) {
        if (firstEvictWarn)
            return;

        // Do not move warning output to synchronized block (it causes warning in IDE).
        synchronized (this) {
            if (firstEvictWarn)
                return;

            firstEvictWarn = true;
        }

        U.warn(log, "Page-based evictions started." +
                " Consider increasing 'maxSize' on Data Region configuration: " + regCfg.getName());
    }
}
