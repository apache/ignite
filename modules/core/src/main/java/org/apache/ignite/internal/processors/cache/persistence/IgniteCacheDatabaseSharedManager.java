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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.management.InstanceNotFoundException;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.DataRegionMetricsProvider;
import org.apache.ignite.DataStorageMetrics;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.systemview.walker.PagesListViewWalker;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.mem.file.MappedFileMemoryProvider;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.processors.cache.persistence.evict.FairFifoPageEvictionTracker;
import org.apache.ignite.internal.processors.cache.persistence.evict.NoOpPageEvictionTracker;
import org.apache.ignite.internal.processors.cache.persistence.evict.PageEvictionTracker;
import org.apache.ignite.internal.processors.cache.persistence.evict.Random2LruPageEvictionTracker;
import org.apache.ignite.internal.processors.cache.persistence.evict.RandomLruPageEvictionTracker;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.freelist.AbstractFreeList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.CacheFreeList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.util.TimeBag;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.mxbean.DataRegionMetricsMXBean;
import org.apache.ignite.spi.systemview.view.PagesListView;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_REUSE_MEMORY_ON_DEACTIVATE;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_MAX_SIZE;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_HISTORY_SIZE;
import static org.apache.ignite.internal.processors.cache.mvcc.txlog.TxLog.TX_LOG_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.METASTORE_DATA_REGION_NAME;

/**
 *
 */
public class IgniteCacheDatabaseSharedManager extends GridCacheSharedManagerAdapter
    implements IgniteChangeGlobalStateSupport, CheckpointLockStateChecker {
    /** DataRegionConfiguration name reserved for internal caches. */
    public static final String SYSTEM_DATA_REGION_NAME = "sysMemPlc";

    /** DataRegionConfiguration names reserved for various internal needs. */
    public static Set<String> INTERNAL_DATA_REGION_NAMES = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(SYSTEM_DATA_REGION_NAME, TX_LOG_CACHE_NAME, METASTORE_DATA_REGION_NAME)));

    /** System view name for page lists. */
    public static final String DATA_REGION_PAGE_LIST_VIEW = "dataRegionPageLists";

    /** System view description for page lists. */
    public static final String DATA_REGION_PAGE_LIST_VIEW_DESC = "Data region page lists";

    /** Minimum size of memory chunk */
    private static final long MIN_PAGE_MEMORY_SIZE = 10L * 1024 * 1024;

    /** Maximum initial size on 32-bit JVM */
    private static final long MAX_PAGE_MEMORY_INIT_SIZE_32_BIT = 2L * 1024 * 1024 * 1024;

    /** {@code True} to reuse memory on deactive. */
    protected final boolean reuseMemory = IgniteSystemProperties.getBoolean(IGNITE_REUSE_MEMORY_ON_DEACTIVATE);

    /** */
    protected final Map<String, DataRegion> dataRegionMap = new ConcurrentHashMap<>();

    /** Stores memory providers eligible for reuse. */
    private final Map<String, DirectMemoryProvider> memProviderMap = new ConcurrentHashMap<>();

    /** */
    private static final String MBEAN_GROUP_NAME = "DataRegionMetrics";

    /** */
    protected final Map<String, DataRegionMetrics> memMetricsMap = new ConcurrentHashMap<>();

    /** */
    private volatile boolean dataRegionsInitialized;

    /** */
    private volatile boolean dataRegionsStarted;

    /** */
    protected DataRegion dfltDataRegion;

    /** */
    protected Map<String, CacheFreeList> freeListMap;

    /** */
    private CacheFreeList dfltFreeList;

    /** Page size from memory configuration, may be set only for fake(standalone) IgniteCacheDataBaseSharedManager */
    private int pageSize;

    /** First eviction was warned flag. */
    private volatile boolean firstEvictWarn;


    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        if (cctx.kernalContext().clientNode() && cctx.kernalContext().config().getDataStorageConfiguration() == null)
            return;

        DataStorageConfiguration memCfg = cctx.kernalContext().config().getDataStorageConfiguration();

        assert memCfg != null;

        validateConfiguration(memCfg);

        pageSize = memCfg.getPageSize();

        initDataRegions(memCfg);

        cctx.kernalContext().systemView().registerView(
            DATA_REGION_PAGE_LIST_VIEW,
            DATA_REGION_PAGE_LIST_VIEW_DESC,
            new PagesListViewWalker(),
            () -> {
                Map<String, CacheFreeList> freeLists = freeListMap;

                if (freeLists == null)
                    return Collections.emptyList();

                return freeLists.values().stream().flatMap(fl -> IntStream.range(0, fl.bucketsCount()).mapToObj(
                    bucket -> new PagesListView(fl, bucket))).collect(Collectors.toList());
            },
            Function.identity()
        );
    }

    /**
     * @param cfg Ignite configuration.
     * @param groupName Name of group.
     * @param dataRegionName Metrics MBean name.
     * @param impl Metrics implementation.
     * @param clazz Metrics class type.
     */
    protected <T> void registerMetricsMBean(
        IgniteConfiguration cfg,
        String groupName,
        String dataRegionName,
        T impl,
        Class<T> clazz
    ) {
        if (U.IGNITE_MBEANS_DISABLED)
            return;

        try {
            U.registerMBean(
                cfg.getMBeanServer(),
                cfg.getIgniteInstanceName(),
                groupName,
                dataRegionName,
                impl,
                clazz);
        }
        catch (Throwable e) {
            U.error(log, "Failed to register MBean with name: " + dataRegionName, e);
        }
    }

    /**
     * @param cfg Ignite configuration.
     * @param groupName Name of group.
     * @param name Name of MBean.
     */
    protected void unregisterMetricsMBean(
        IgniteConfiguration cfg,
        String groupName,
        String name
    ) {
        if (U.IGNITE_MBEANS_DISABLED)
            return;

        assert cfg != null;

        try {
            cfg.getMBeanServer().unregisterMBean(
                U.makeMBeanName(
                    cfg.getIgniteInstanceName(),
                    groupName,
                    name
                ));
        }
        catch (InstanceNotFoundException ignored) {
            // We tried to unregister a non-existing MBean, not a big deal.
        }
        catch (Throwable e) {
            U.error(log, "Failed to unregister MBean for memory metrics: " + name, e);
        }
    }

    /**
     * Registers MBeans for all DataRegionMetrics configured in this instance.
     *
     * @param cfg Ignite configuration.
     */
    protected void registerMetricsMBeans(IgniteConfiguration cfg) {
        if (U.IGNITE_MBEANS_DISABLED)
            return;

        assert cfg != null;

        for (DataRegionMetrics memMetrics : memMetricsMap.values()) {
            DataRegionConfiguration memPlcCfg = dataRegionMap.get(memMetrics.getName()).config();

            registerMetricsMBean(
                cfg,
                MBEAN_GROUP_NAME,
                memPlcCfg.getName(),
                new DataRegionMetricsMXBeanImpl((DataRegionMetricsImpl)memMetrics, memPlcCfg),
                DataRegionMetricsMXBean.class
            );
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

            DataRegionMetricsImpl memMetrics = (DataRegionMetricsImpl)memMetricsMap.get(memPlcCfg.getName());

            boolean persistenceEnabled = memPlcCfg.isPersistenceEnabled();

            String freeListName = memPlcCfg.getName() + "##FreeList";

            PageLockListener lsnr = cctx.diagnostic().pageLockTracker().createPageLockTracker(freeListName);

            CacheFreeList freeList = new CacheFreeList(
                0,
                freeListName,
                memMetrics,
                memPlc,
                persistenceEnabled ? cctx.wal() : null,
                0L,
                true,
                lsnr,
                cctx.kernalContext(),
                null
            );

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
    private void startDataRegions() {
        for (DataRegion region : dataRegionMap.values()) {
            if (!cctx.isLazyMemoryAllocation(region))
                region.pageMemory().start();

            region.evictionTracker().start();
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

        U.log(log, "Configured data regions initialized successfully [total=" + dataRegionMap.size() + ']');
    }

    /**
     * @param memCfg Database config.
     * @throws IgniteCheckedException If failed to initialize swap path.
     */
    protected void initDataRegions0(DataStorageConfiguration memCfg) throws IgniteCheckedException {
        DataRegionConfiguration[] dataRegionCfgs = memCfg.getDataRegionConfigurations();

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

        DataRegionMetricsImpl memMetrics = new DataRegionMetricsImpl(
            dataRegionCfg,
            cctx.kernalContext().metric(),
            dataRegionMetricsProvider(dataRegionCfg));

        DataRegion region = initMemory(dataStorageCfg, dataRegionCfg, memMetrics, trackable);

        dataRegionMap.put(dataRegionName, region);

        memMetricsMap.put(dataRegionName, memMetrics);

        if (dataRegionName.equals(dfltMemPlcName))
            dfltDataRegion = region;
        else if (dataRegionName.equals(DFLT_DATA_REG_DEFAULT_NAME))
            U.warn(log, "Data Region with name 'default' isn't used as a default. " +
                    "Please, check Data Region configuration.");
    }

    /**
     * Closure that can be used to compute fill factor for provided data region.
     *
     * @param dataRegCfg Data region configuration.
     * @return Closure.
     *
     * @deprecated use {@link #dataRegionMetricsProvider(DataRegionConfiguration)} instead.
     */
    @Deprecated
    protected IgniteOutClosure<Long> freeSpaceProvider(final DataRegionConfiguration dataRegCfg) {
        final String dataRegName = dataRegCfg.getName();

        return new IgniteOutClosure<Long>() {
            private CacheFreeList freeList;

            @Override public Long apply() {
                if (freeList == null) {
                    CacheFreeList freeList0 = freeListMap.get(dataRegName);

                    if (freeList0 == null)
                        return 0L;

                    freeList = freeList0;
                }

                return freeList.freeSpace();
            }
        };
    }

    /**
     * Provide that can be used to compute some metrics for provided data region.
     *
     * @param dataRegCfg Data region configuration.
     * @return DataRegionMetricsProvider.
     */
    protected DataRegionMetricsProvider dataRegionMetricsProvider(final DataRegionConfiguration dataRegCfg) {
        final String dataRegName = dataRegCfg.getName();

        return new DataRegionMetricsProvider() {
            private CacheFreeList freeList;

            private CacheFreeList getFreeList() {
                if (freeListMap == null)
                    return null;

                if (freeList == null)
                    freeList = freeListMap.get(dataRegName);

                return freeList;
            }

            @Override public long partiallyFilledPagesFreeSpace() {
                CacheFreeList freeList0 = getFreeList();

                return freeList0 == null ? 0L : freeList0.freeSpace();
            }

            @Override public long emptyDataPages() {
                CacheFreeList freeList0 = getFreeList();

                return freeList0 == null ? 0L : freeList0.emptyDataPages();
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
        res.setLazyMemoryAllocation(false);

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
        else if (memCfg.getMaxWalArchiveSize() == DFLT_WAL_ARCHIVE_MAX_SIZE)
            LT.warn(log, "walHistorySize was deprecated. maxWalArchiveSize should be used instead");
        else
            throw new IgniteCheckedException("Should be used only one of wal history size or max wal archive size." +
                "(use DataRegionConfiguration.maxWalArchiveSize because DataRegionConfiguration.walHistorySize was deprecated)"
            );

        if (memCfg.getMaxWalArchiveSize() < memCfg.getWalSegmentSize())
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

        if (INTERNAL_DATA_REGION_NAMES.contains(regName))
            throw new IgniteCheckedException("'" + regName + "' policy name is reserved for internal use.");

        observedNames.add(regName);
    }

    /**
     * @param log Logger.
     */
    public void dumpStatistics(IgniteLogger log) {
        if (freeListMap != null) {
            for (CacheFreeList freeList : freeListMap.values())
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
     * @param memPlcName Name of {@link DataRegion} to obtain {@link DataRegionMetrics} for.
     * @return {@link DataRegionMetrics} snapshot for specified {@link DataRegion} or {@code null} if
     * no {@link DataRegion} is configured for specified name.
     */
    public @Nullable DataRegionMetrics memoryMetrics(String memPlcName) {
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

        if (dataRegionMap.isEmpty())
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
     * @return {@code 0} for non-persistent storage.
     */
    public long checkpointReadLockTimeout() {
        return 0;
    }

    /**
     * No-op for non-persistent storage.
     */
    public void checkpointReadLockTimeout(long val) {
        // No-op.
    }

    /**
     * Method will perform cleanup cache page memory and each cache partition store.
     */
    public void cleanupRestoredCaches() {
        // No-op.
    }

    /**
     * Clean checkpoint directory {@link GridCacheDatabaseSharedManager#cpDir}. The operation
     * is necessary when local node joined to baseline topology with different consistentId.
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
    @Nullable public CheckpointProgress forceCheckpoint(String reason) {
        return null;
    }

    /**
     * Waits until current state is checkpointed.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void waitForCheckpoint(String reason) throws IgniteCheckedException {
        waitForCheckpoint(reason, null);
    }

    /**
     * Waits until current state is checkpointed and execution listeners after finish.
     *
     * @param reason Reason for checkpoint wakeup if it would be required.
     * @param lsnr Listeners which should be called in checkpoint thread after current checkpoint finished.
     * @throws IgniteCheckedException If failed.
     */
    public <R> void waitForCheckpoint(
        String reason,
        IgniteInClosure<? super IgniteInternalFuture<R>> lsnr
    ) throws IgniteCheckedException {
        // No-op
    }

    /**
     * @param discoEvt Before exchange for the given discovery event.
     */
    public void beforeExchange(GridDhtPartitionsExchangeFuture discoEvt) throws IgniteCheckedException {

    }

    /**
     * Perform memory restore before {@link GridDiscoveryManager} start.
     *
     * @param kctx Current kernal context.
     * @param startTimer Holder of start time of stages.
     * @throws IgniteCheckedException If fails.
     */
    public void startMemoryRestore(GridKernalContext kctx, TimeBag startTimer) throws IgniteCheckedException {
        // No-op.
    }

    /**
     * Called when all partitions have been fully restored and pre-created on node start.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void onStateRestored(AffinityTopologyVersion topVer) throws IgniteCheckedException {
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
     *
     * @param reservationMap Map contains of counters for partitions of groups.
     * @return True if successfully reserved.
     */
    public boolean reserveHistoryForPreloading(Map<T2<Integer, Integer>, Long> reservationMap) {
        return false;
    }

    /**
     * Release reserved update history.
     */
    public void releaseHistoryForPreloading() {
        // No-op
    }

    /**
     * Checks that the given {@code region} has enough space for putting a new entry.
     *
     * This method makes sense then and only then
     * the data region is not persisted {@link DataRegionConfiguration#isPersistenceEnabled()}
     * and page eviction is disabled {@link DataPageEvictionMode#DISABLED}.
     *
     * The non-persistent region should reserve a number of pages to support a free list {@link AbstractFreeList}.
     * For example, removing a row from underlying store may require allocating a new data page
     * in order to move a tracked page from one bucket to another one which does not have a free space for a new stripe.
     * See {@link AbstractFreeList#removeDataRowByLink}.
     * Therefore, inserting a new entry should be prevented in case of some threshold is exceeded.
     *
     * @param region Data region to be checked.
     * @param dataRowSize Size of data row to be inserted.
     * @throws IgniteOutOfMemoryException In case of the given data region does not have enough free space
     * for putting a new entry.
     */
    public void ensureFreeSpaceForInsert(DataRegion region, int dataRowSize) throws IgniteOutOfMemoryException {
        if (region == null)
            return;

        DataRegionConfiguration regCfg = region.config();

        if (regCfg.getPageEvictionMode() != DataPageEvictionMode.DISABLED || regCfg.isPersistenceEnabled())
            return;

        long memorySize = regCfg.getMaxSize();

        PageMemory pageMem = region.pageMemory();

        CacheFreeList freeList = freeListMap.get(regCfg.getName());

        long nonEmptyPages = (pageMem.loadedPages() - freeList.emptyDataPages());

        // The maximum number of pages that can be allocated (memorySize / systemPageSize)
        // should be greater or equal to pages required for inserting a new entry plus
        // the current number of non-empty pages plus the number of pages that may be required in order to move
        // all pages to a reuse bucket, that is equal to nonEmptyPages * 8 / pageSize, where 8 is the size of a link.
        // Note that not the whole page can be used to storing links,
        // see PagesListNodeIO and PagesListMetaIO#getCapacity(), so we pessimistically multiply the result on 1.5,
        // in any way, the number of required pages is less than 1 percent.
        boolean oomThreshold = (memorySize / pageMem.systemPageSize()) <
            ((double)dataRowSize / pageMem.pageSize() + nonEmptyPages * (8.0 * 1.5 / pageMem.pageSize() + 1) + 256 /*one page per bucket*/);

        if (oomThreshold) {
            IgniteOutOfMemoryException oom = new IgniteOutOfMemoryException("Out of memory in data region [" +
                "name=" + regCfg.getName() +
                ", initSize=" + U.readableSize(regCfg.getInitialSize(), false) +
                ", maxSize=" + U.readableSize(regCfg.getMaxSize(), false) +
                ", persistenceEnabled=" + regCfg.isPersistenceEnabled() + "] Try the following:" + U.nl() +
                "  ^-- Increase maximum off-heap memory size (DataRegionConfiguration.maxSize)" + U.nl() +
                "  ^-- Enable Ignite persistence (DataRegionConfiguration.persistenceEnabled)" + U.nl() +
                "  ^-- Enable eviction or expiration policies"
            );

            if (cctx.kernalContext() != null)
                cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, oom));

            throw oom;
        }
    }

    /**
     * See {@link GridCacheMapEntry#ensureFreeSpace()}
     *
     * @param memPlc data region.
     */
    public void ensureFreeSpace(DataRegion memPlc) throws IgniteCheckedException {
        if (memPlc == null)
            return;

        while (memPlc.evictionTracker().evictionRequired()) {
            warnFirstEvict(memPlc.config());

            memPlc.evictionTracker().evictDataPage();

            memPlc.memoryMetrics().updateEvictionRate();
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
    public boolean supportsMemoryReuse(DataRegionConfiguration plcCfg) {
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
            memMetrics.totalAllocatedPages(),
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
        if (kctx.clientNode() && kctx.config().getDataStorageConfiguration() == null)
            return;

        initAndStartRegions(kctx.config().getDataStorageConfiguration());

        for (DatabaseLifecycleListener lsnr : getDatabaseListeners(kctx))
            lsnr.afterInitialise(this);
    }

    /**
     * @param cfg Current data storage configuration.
     * @throws IgniteCheckedException If fails.
     */
    protected void initAndStartRegions(DataStorageConfiguration cfg) throws IgniteCheckedException {
        assert cfg != null;

        initDataRegions(cfg);

        startDataRegions(cfg);
    }

    /**
     * @param cfg Regions configuration.
     * @throws IgniteCheckedException If fails.
     */
    private void startDataRegions(DataStorageConfiguration cfg) throws IgniteCheckedException {
        if (dataRegionsStarted)
            return;

        assert cfg != null;

        registerMetricsMBeans(cctx.gridConfig());

        startDataRegions();

        initPageMemoryDataStructures(cfg);

        dataRegionsStarted = true;

        if (log.isQuiet()) {
            U.quiet(false, "Data Regions Started: " + dataRegionMap.size());

            U.quietMultipleLines(false, IgniteKernal.dataStorageReport(this, false));
        }
        else if (log.isInfoEnabled()) {
            log.info("Data Regions Started: " + dataRegionMap.size());

            log.info(IgniteKernal.dataStorageReport(this, false));
        }
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        onDeActivate(!reuseMemory);
    }

    /**
     * @param shutdown {@code True} to force memory regions shutdown.
     */
    private void onDeActivate(boolean shutdown) {
        for (DatabaseLifecycleListener lsnr : getDatabaseListeners(cctx.kernalContext()))
            lsnr.beforeStop(this);

        for (DataRegion region : dataRegionMap.values()) {
            region.pageMemory().stop(shutdown);

            region.evictionTracker().stop();

            unregisterMetricsMBean(
                cctx.gridConfig(),
                MBEAN_GROUP_NAME,
                region.memoryMetrics().getName()
            );
        }

        dataRegionMap.clear();

        if (shutdown && memProviderMap != null)
            memProviderMap.clear();

        dataRegionsInitialized = false;

        dataRegionsStarted = false;
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
