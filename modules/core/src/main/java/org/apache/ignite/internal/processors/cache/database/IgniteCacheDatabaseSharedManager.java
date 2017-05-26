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

package org.apache.ignite.internal.processors.cache.database;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.management.JMException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.MemoryMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.file.MappedFileMemoryProvider;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.pagemem.snapshot.StartFullSnapshotAckDiscoveryMessage;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.database.evict.FairFifoPageEvictionTracker;
import org.apache.ignite.internal.processors.cache.database.evict.NoOpPageEvictionTracker;
import org.apache.ignite.internal.processors.cache.database.evict.PageEvictionTracker;
import org.apache.ignite.internal.processors.cache.database.evict.Random2LruPageEvictionTracker;
import org.apache.ignite.internal.processors.cache.database.evict.RandomLruPageEvictionTracker;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeListImpl;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.mxbean.MemoryMetricsMXBean;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.configuration.MemoryConfiguration.DFLT_MEMORY_POLICY_INITIAL_SIZE;
import static org.apache.ignite.configuration.MemoryConfiguration.DFLT_MEM_PLC_DEFAULT_NAME;

/**
 *
 */
public class IgniteCacheDatabaseSharedManager extends GridCacheSharedManagerAdapter implements IgniteChangeGlobalStateSupport {
    /** MemoryPolicyConfiguration name reserved for internal caches. */
    static final String SYSTEM_MEMORY_POLICY_NAME = "sysMemPlc";

    /** Minimum size of memory chunk */
    private static final long MIN_PAGE_MEMORY_SIZE = 10 * 1024 * 1024;

    /** Maximum initial size on 32-bit JVM */
    private static final long MAX_PAGE_MEMORY_INIT_SIZE_32_BIT = 2L * 1024 * 1024 * 1024;

    /** */
    protected Map<String, MemoryPolicy> memPlcMap;

    /** */
    protected Map<String, MemoryMetrics> memMetricsMap;

    /** */
    protected MemoryPolicy dfltMemPlc;

    /** */
    private Map<String, FreeListImpl> freeListMap;

    /** */
    private FreeListImpl dfltFreeList;

    /** */
    private int pageSize;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        if (cctx.kernalContext().clientNode() && cctx.kernalContext().config().getMemoryConfiguration() == null)
            return;

        init();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void init() throws IgniteCheckedException {
        if (memPlcMap == null) {
            MemoryConfiguration memCfg = cctx.kernalContext().config().getMemoryConfiguration();

            if (memCfg == null)
                memCfg = new MemoryConfiguration();

            validateConfiguration(memCfg);

            pageSize = memCfg.getPageSize();

            initPageMemoryPolicies(memCfg);

            registerMetricsMBeans();

            startMemoryPolicies();

            initPageMemoryDataStructures(memCfg);
        }
    }

    /**
     * Registers MBeans for all MemoryMetrics configured in this instance.
     */
    private void registerMetricsMBeans() {
        IgniteConfiguration cfg = cctx.gridConfig();

        for (MemoryMetrics memMetrics : memMetricsMap.values()) {
            MemoryPolicyConfiguration memPlcCfg = memPlcMap.get(memMetrics.getName()).config();

            registerMetricsMBean((MemoryMetricsImpl)memMetrics, memPlcCfg, cfg);
        }
    }

    /**
     * @param memMetrics Memory metrics.
     * @param memPlcCfg Memory policy configuration.
     * @param cfg Ignite configuration.
     */
    private void registerMetricsMBean(MemoryMetricsImpl memMetrics,
        MemoryPolicyConfiguration memPlcCfg,
        IgniteConfiguration cfg) {
        try {
            U.registerMBean(
                    cfg.getMBeanServer(),
                    cfg.getIgniteInstanceName(),
                    "MemoryMetrics",
                    memPlcCfg.getName(),
                    new MemoryMetricsMXBeanImpl(memMetrics, memPlcCfg),
                    MemoryMetricsMXBean.class);
        }
        catch (JMException e) {
            U.error(log, "Failed to register MBean for MemoryMetrics with name: '" + memMetrics.getName() + "'", e);
        }
    }

    /**
     * @param dbCfg Database config.
     */
    protected void initPageMemoryDataStructures(MemoryConfiguration dbCfg) throws IgniteCheckedException {
        freeListMap = U.newHashMap(memPlcMap.size());

        String dfltMemPlcName = dbCfg.getDefaultMemoryPolicyName();

        for (MemoryPolicy memPlc : memPlcMap.values()) {
            MemoryPolicyConfiguration memPlcCfg = memPlc.config();

            MemoryMetricsImpl memMetrics = (MemoryMetricsImpl) memMetricsMap.get(memPlcCfg.getName());

            FreeListImpl freeList = new FreeListImpl(0,
                    cctx.igniteInstanceName(),
                    memMetrics,
                    memPlc,
                    null,
                    cctx.wal(),
                    0L,
                    true);

            memMetrics.freeList(freeList);

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
        for (MemoryPolicy memPlc : memPlcMap.values()) {
            memPlc.pageMemory().start();

            memPlc.evictionTracker().start();
        }
    }

    /**
     * @param memCfg Database config.
     */
    protected void initPageMemoryPolicies(MemoryConfiguration memCfg) {
        MemoryPolicyConfiguration[] memPlcsCfgs = memCfg.getMemoryPolicies();

        if (memPlcsCfgs == null) {
            //reserve place for default and system memory policies
            memPlcMap = U.newHashMap(2);
            memMetricsMap = U.newHashMap(2);

            addMemoryPolicy(memCfg,
                memCfg.createDefaultPolicyConfig(),
                DFLT_MEM_PLC_DEFAULT_NAME);

            U.warn(log, "No user-defined default MemoryPolicy found; system default of 1GB size will be used.");
        }
        else {
            String dfltMemPlcName = memCfg.getDefaultMemoryPolicyName();

            if (DFLT_MEM_PLC_DEFAULT_NAME.equals(dfltMemPlcName) && !hasCustomDefaultMemoryPolicy(memPlcsCfgs)) {
                //reserve additional place for default and system memory policies
                memPlcMap = U.newHashMap(memPlcsCfgs.length + 2);
                memMetricsMap = U.newHashMap(memPlcsCfgs.length + 2);

                addMemoryPolicy(memCfg,
                    memCfg.createDefaultPolicyConfig(),
                    DFLT_MEM_PLC_DEFAULT_NAME);

                U.warn(log, "No user-defined default MemoryPolicy found; system default of 1GB size will be used.");
            }
            else {
                //reserve additional space for system memory policy only
                memPlcMap = U.newHashMap(memPlcsCfgs.length + 1);
                memMetricsMap = U.newHashMap(memPlcsCfgs.length + 1);
            }

            for (MemoryPolicyConfiguration memPlcCfg : memPlcsCfgs)
                addMemoryPolicy(memCfg, memPlcCfg, memPlcCfg.getName());
        }

        addMemoryPolicy(memCfg,
            createSystemMemoryPolicy(memCfg.getSystemCacheInitialSize(), memCfg.getSystemCacheMaxSize()),
            SYSTEM_MEMORY_POLICY_NAME);
    }

    /**
     * @param dbCfg Database config.
     * @param memPlcCfg Memory policy config.
     * @param memPlcName Memory policy name.
     */
    private void addMemoryPolicy(MemoryConfiguration dbCfg,
                                 MemoryPolicyConfiguration memPlcCfg,
                                 String memPlcName) {
        String dfltMemPlcName = dbCfg.getDefaultMemoryPolicyName();

        if (dfltMemPlcName == null)
            dfltMemPlcName = DFLT_MEM_PLC_DEFAULT_NAME;

        MemoryMetricsImpl memMetrics = new MemoryMetricsImpl(memPlcCfg);

        MemoryPolicy memPlc = initMemory(dbCfg, memPlcCfg, memMetrics);

        memPlcMap.put(memPlcName, memPlc);

        memMetricsMap.put(memPlcName, memMetrics);

        if (memPlcName.equals(dfltMemPlcName))
            dfltMemPlc = memPlc;
        else if (memPlcName.equals(DFLT_MEM_PLC_DEFAULT_NAME))
            U.warn(log, "Memory Policy with name 'default' isn't used as a default. " +
                    "Please check Memory Policies configuration.");
    }

    /**
     * @param memPlcsCfgs User-defined memory policy configurations.
     */
    private boolean hasCustomDefaultMemoryPolicy(MemoryPolicyConfiguration[] memPlcsCfgs) {
        for (MemoryPolicyConfiguration memPlcsCfg : memPlcsCfgs) {
            if (DFLT_MEM_PLC_DEFAULT_NAME.equals(memPlcsCfg.getName()))
                return true;
        }

        return false;
    }

    /**
     * @param dbCfg Database configuration.
     * @param memPlcCfg MemoryPolicy configuration.
     * @param memMetrics MemoryMetrics instance.
     */
    private MemoryPolicy createDefaultMemoryPolicy(MemoryConfiguration dbCfg, MemoryPolicyConfiguration memPlcCfg, MemoryMetricsImpl memMetrics) {
        return initMemory(dbCfg, memPlcCfg, memMetrics);
    }

    /**
     * @param sysCacheInitSize Initial size of PageMemory to be created for system cache.
     * @param sysCacheMaxSize Maximum size of PageMemory to be created for system cache.
     *
     * @return {@link MemoryPolicyConfiguration configuration} of MemoryPolicy for system cache.
     */
    private MemoryPolicyConfiguration createSystemMemoryPolicy(long sysCacheInitSize, long sysCacheMaxSize) {
        MemoryPolicyConfiguration res = new MemoryPolicyConfiguration();

        res.setName(SYSTEM_MEMORY_POLICY_NAME);
        res.setInitialSize(sysCacheInitSize);
        res.setMaxSize(sysCacheMaxSize);

        return res;
    }

    /**
     * @param memCfg configuration to validate.
     */
    private void validateConfiguration(MemoryConfiguration memCfg) throws IgniteCheckedException {
        MemoryPolicyConfiguration[] plcCfgs = memCfg.getMemoryPolicies();

        Set<String> plcNames = (plcCfgs != null) ? U.<String>newHashSet(plcCfgs.length) : new HashSet<String>(0);

        checkSystemMemoryPolicySizeConfiguration(memCfg.getSystemCacheInitialSize(),
            memCfg.getSystemCacheMaxSize());

        if (plcCfgs != null) {
            for (MemoryPolicyConfiguration plcCfg : plcCfgs) {
                assert plcCfg != null;

                checkPolicyName(plcCfg.getName(), plcNames);

                checkPolicySize(plcCfg);

                checkMetricsProperties(plcCfg);

                checkPolicyEvictionProperties(plcCfg, memCfg);
            }
        }

        checkDefaultPolicyConfiguration(
                memCfg.getDefaultMemoryPolicyName(),
                memCfg.getDefaultMemoryPolicySize(),
                plcNames);
    }

    /**
     * @param plcCfg Memory policy config.
     *
     * @throws IgniteCheckedException if validation of memory metrics properties fails.
     */
    private static void checkMetricsProperties(MemoryPolicyConfiguration plcCfg) throws IgniteCheckedException {
        if (plcCfg.getRateTimeInterval() <= 0)
            throw new IgniteCheckedException("Rate time interval must be greater than zero " +
                "(use MemoryPolicyConfiguration.rateTimeInterval property to adjust the interval) " +
                "[name=" + plcCfg.getName() +
                ", rateTimeInterval=" + plcCfg.getRateTimeInterval() + "]"
            );
        if (plcCfg.getSubIntervals() <= 0)
            throw new IgniteCheckedException("Sub intervals must be greater than zero " +
                "(use MemoryPolicyConfiguration.subIntervals property to adjust the sub intervals) " +
                "[name=" + plcCfg.getName() +
                ", subIntervals=" + plcCfg.getSubIntervals() + "]"
            );
    }

    /**
     * @param sysCacheInitSize System cache initial size.
     * @param sysCacheMaxSize System cache max size.
     *
     * @throws IgniteCheckedException In case of validation violation.
     */
    private void checkSystemMemoryPolicySizeConfiguration(long sysCacheInitSize, long sysCacheMaxSize) throws IgniteCheckedException {
        if (sysCacheInitSize < MIN_PAGE_MEMORY_SIZE)
            throw new IgniteCheckedException("Initial size for system cache must have size more than 10MB (use " +
                "MemoryConfiguration.systemCacheInitialSize property to set correct size in bytes) " +
                "[size=" + U.readableSize(sysCacheInitSize, true) + ']'
            );

        if (U.jvm32Bit() && sysCacheInitSize > MAX_PAGE_MEMORY_INIT_SIZE_32_BIT)
            throw new IgniteCheckedException("Initial size for system cache exceeds 2GB on 32-bit JVM (use " +
                "MemoryPolicyConfiguration.systemCacheInitialSize property to set correct size in bytes " +
                "or use 64-bit JVM) [size=" + U.readableSize(sysCacheInitSize, true) + ']'
            );

        if (sysCacheMaxSize < sysCacheInitSize)
            throw new IgniteCheckedException("MaxSize of system cache must not be smaller than " +
                "initialSize [initSize=" + U.readableSize(sysCacheInitSize, true) +
                ", maxSize=" + U.readableSize(sysCacheMaxSize, true) + "]. " +
                "Use MemoryConfiguration.systemCacheInitialSize/MemoryConfiguration.systemCacheMaxSize " +
                "properties to set correct sizes in bytes."
            );
    }

    /**
     * @param dfltPlcName Default MemoryPolicy name.
     * @param dfltPlcSize Default size of MemoryPolicy overridden by user (equals to -1 if wasn't specified by user).
     * @param plcNames All MemoryPolicy names.
     * @throws IgniteCheckedException In case of validation violation.
     */
    private static void checkDefaultPolicyConfiguration(
        String dfltPlcName,
        long dfltPlcSize,
        Collection<String> plcNames
    ) throws IgniteCheckedException {
        if (dfltPlcSize != -1) {
            if (!F.eq(dfltPlcName, MemoryConfiguration.DFLT_MEM_PLC_DEFAULT_NAME))
                throw new IgniteCheckedException("User-defined MemoryPolicy configuration " +
                    "and defaultMemoryPolicySize properties are set at the same time. " +
                    "Delete either MemoryConfiguration.defaultMemoryPolicySize property " +
                    "or user-defined default MemoryPolicy configuration");

            if (dfltPlcSize < MIN_PAGE_MEMORY_SIZE)
                throw new IgniteCheckedException("User-defined default MemoryPolicy size is less than 1MB. " +
                        "Use MemoryConfiguration.defaultMemoryPolicySize property to set correct size.");

            if (U.jvm32Bit() && dfltPlcSize > MAX_PAGE_MEMORY_INIT_SIZE_32_BIT)
                throw new IgniteCheckedException("User-defined default MemoryPolicy size exceeds 2GB on 32-bit JVM " +
                    "(use MemoryConfiguration.defaultMemoryPolicySize property to set correct size in bytes " +
                    "or use 64-bit JVM) [size=" + U.readableSize(dfltPlcSize, true) + ']'
                );
        }

        if (!DFLT_MEM_PLC_DEFAULT_NAME.equals(dfltPlcName)) {
            if (dfltPlcName.isEmpty())
                throw new IgniteCheckedException("User-defined default MemoryPolicy name must be non-empty");

            if (!plcNames.contains(dfltPlcName))
                throw new IgniteCheckedException("User-defined default MemoryPolicy name " +
                    "must be presented among configured MemoryPolices: " + dfltPlcName);
        }
    }

    /**
     * @param plcCfg MemoryPolicyConfiguration to validate.
     * @throws IgniteCheckedException If config is invalid.
     */
    private void checkPolicySize(MemoryPolicyConfiguration plcCfg) throws IgniteCheckedException {
        if (plcCfg.getInitialSize() < MIN_PAGE_MEMORY_SIZE)
            throw new IgniteCheckedException("MemoryPolicy must have size more than 10MB (use " +
                "MemoryPolicyConfiguration.initialSize property to set correct size in bytes) " +
                "[name=" + plcCfg.getName() + ", size=" + U.readableSize(plcCfg.getInitialSize(), true) + "]"
            );

        if (plcCfg.getMaxSize() < plcCfg.getInitialSize()) {
            // We will know for sure if initialSize has been changed if we compare Longs by "==".
            if (plcCfg.getInitialSize() == DFLT_MEMORY_POLICY_INITIAL_SIZE) {
                plcCfg.setInitialSize(plcCfg.getMaxSize());

                LT.warn(log, "MemoryPolicy maxSize=" + U.readableSize(plcCfg.getMaxSize(), true) +
                    " is smaller than defaultInitialSize=" +
                    U.readableSize(MemoryConfiguration.DFLT_MEMORY_POLICY_INITIAL_SIZE, true) +
                    ", setting initialSize to " + U.readableSize(plcCfg.getMaxSize(), true));
            }
            else {
                throw new IgniteCheckedException("MemoryPolicy maxSize must not be smaller than " +
                    "initialSize [name=" + plcCfg.getName() +
                    ", initSize=" + U.readableSize(plcCfg.getInitialSize(), true) +
                    ", maxSize=" + U.readableSize(plcCfg.getMaxSize(), true) + ']');
            }
        }

        if (U.jvm32Bit() && plcCfg.getInitialSize() > MAX_PAGE_MEMORY_INIT_SIZE_32_BIT)
            throw new IgniteCheckedException("MemoryPolicy initialSize exceeds 2GB on 32-bit JVM (use " +
                "MemoryPolicyConfiguration.initialSize property to set correct size in bytes or use 64-bit JVM) " +
                "[name=" + plcCfg.getName() +
                ", size=" + U.readableSize(plcCfg.getInitialSize(), true) + "]");
    }

    /**
     * @param plcCfg MemoryPolicyConfiguration to validate.
     * @param dbCfg Memory configuration.
     * @throws IgniteCheckedException If config is invalid.
     */
    protected void checkPolicyEvictionProperties(MemoryPolicyConfiguration plcCfg, MemoryConfiguration dbCfg)
        throws IgniteCheckedException {
        if (plcCfg.getPageEvictionMode() == DataPageEvictionMode.DISABLED)
            return;

        if (plcCfg.getEvictionThreshold() < 0.5 || plcCfg.getEvictionThreshold() > 0.999) {
            throw new IgniteCheckedException("Page eviction threshold must be between 0.5 and 0.999: " +
                plcCfg.getName());
        }

        if (plcCfg.getEmptyPagesPoolSize() <= 10)
            throw new IgniteCheckedException("Evicted pages pool size should be greater than 10: " + plcCfg.getName());

        long maxPoolSize = plcCfg.getMaxSize() / dbCfg.getPageSize() / 10;

        if (plcCfg.getEmptyPagesPoolSize() >= maxPoolSize) {
            throw new IgniteCheckedException("Evicted pages pool size should be lesser than " + maxPoolSize +
                ": " + plcCfg.getName());
        }
    }

    /**
     * @param plcName MemoryPolicy name to validate.
     * @param observedNames Names of MemoryPolicies observed before.
     * @throws IgniteCheckedException If config is invalid.
     */
    private static void checkPolicyName(String plcName, Collection<String> observedNames)
        throws IgniteCheckedException {
        if (plcName == null || plcName.isEmpty())
            throw new IgniteCheckedException("User-defined MemoryPolicyConfiguration must have non-null and " +
                "non-empty name.");

        if (observedNames.contains(plcName))
            throw new IgniteCheckedException("Two MemoryPolicies have the same name: " + plcName);

        if (SYSTEM_MEMORY_POLICY_NAME.equals(plcName))
            throw new IgniteCheckedException("'sysMemPlc' policy name is reserved for internal use.");

        observedNames.add(plcName);
    }

    /**
     * @param log Logger.
     */
    public void dumpStatistics(IgniteLogger log) {
        if (freeListMap != null) {
            for (FreeListImpl freeList : freeListMap.values())
                freeList.dumpStatistics(log);
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void initDataBase() throws IgniteCheckedException{
        // No-op.
    }

    /**
     * @return collection of all configured {@link MemoryPolicy policies}.
     */
    public Collection<MemoryPolicy> memoryPolicies() {
        return memPlcMap != null ? memPlcMap.values() : null;
    }

    /**
     * @return MemoryMetrics for all MemoryPolicies configured in Ignite instance.
     */
    public Collection<MemoryMetrics> memoryMetrics() {
        if (!F.isEmpty(memMetricsMap)) {
            // Intentionally return a collection copy to make it explicitly serializable.
            Collection<MemoryMetrics> res = new ArrayList<>(memMetricsMap.size());

            for (MemoryMetrics metrics : memMetricsMap.values())
                res.add(new MemoryMetricsSnapshot(metrics));

            return res;
        }
        else
            return Collections.emptyList();
    }

    /**
     * @param memPlcName Name of {@link MemoryPolicy} to obtain {@link MemoryMetrics} for.
     * @return {@link MemoryMetrics} snapshot for specified {@link MemoryPolicy} or {@code null} if
     * no {@link MemoryPolicy} is configured for specified name.
     */
    @Nullable public MemoryMetrics memoryMetrics(String memPlcName) {
        if (!F.isEmpty(memMetricsMap)) {
            MemoryMetrics memMetrics = memMetricsMap.get(memPlcName);

            if (memMetrics == null)
                return null;
            else
                return new MemoryMetricsSnapshot(memMetrics);
        }
        else
            return null;
    }

    /**
     * @param memPlcName Memory policy name.
     * @return {@link MemoryPolicy} instance associated with a given {@link MemoryPolicyConfiguration}.
     * @throws IgniteCheckedException in case of request for unknown MemoryPolicy.
     */
    public MemoryPolicy memoryPolicy(String memPlcName) throws IgniteCheckedException {
        if (memPlcName == null)
            return dfltMemPlc;

        if (memPlcMap == null)
            return null;

        MemoryPolicy plc;

        if ((plc = memPlcMap.get(memPlcName)) == null)
            throw new IgniteCheckedException("Requested MemoryPolicy is not configured: " + memPlcName);

        return plc;
    }

    /**
     * @param memPlcName MemoryPolicyConfiguration name.
     * @return {@link FreeList} instance associated with a given {@link MemoryPolicyConfiguration}.
     */
    public FreeList freeList(String memPlcName) {
        if (memPlcName == null)
            return dfltFreeList;

        return freeListMap != null ? freeListMap.get(memPlcName) : null;
    }

    /**
     * @param memPlcName MemoryPolicyConfiguration name.
     * @return {@link ReuseList} instance associated with a given {@link MemoryPolicyConfiguration}.
     */
    public ReuseList reuseList(String memPlcName) {
        if (memPlcName == null)
            return dfltFreeList;

        return freeListMap != null ? freeListMap.get(memPlcName) : null;
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        if (memPlcMap != null) {
            for (MemoryPolicy memPlc : memPlcMap.values()) {
                memPlc.pageMemory().stop();

                memPlc.evictionTracker().stop();

                IgniteConfiguration cfg = cctx.gridConfig();

                try {
                    cfg.getMBeanServer().unregisterMBean(
                        U.makeMBeanName(
                            cfg.getIgniteInstanceName(),
                            "MemoryMetrics",
                            memPlc.memoryMetrics().getName()));
                }
                catch (JMException e) {
                    U.error(log, "Failed to unregister MBean for memory metrics: " +
                        memPlc.memoryMetrics().getName(), e);
                }
            }
        }
    }

    /**
     *
     */
    public boolean persistenceEnabled() {
        return false;
    }

    /**
     *
     */
    public void lock() throws IgniteCheckedException {

    }

    /**
     *
     */
    public void unLock(){

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
     *
     */
    @Nullable public IgniteInternalFuture wakeupForCheckpoint(String reason) {
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
     *
     */
    @Nullable public IgniteInternalFuture wakeupForSnapshot(long snapshotId, UUID snapshotNodeId,
        Collection<String> cacheNames) {
        return null;
    }

    /**
     * @param discoEvt Before exchange for the given discovery event.
     */
    public void beforeExchange(GridDhtPartitionsExchangeFuture discoEvt) throws IgniteCheckedException {
        // No-op.
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void beforeCachesStop() throws IgniteCheckedException {
        // No-op.
    }

    /**
     * @param cctx Stopped cache context.
     */
    public void onCacheStop(GridCacheContext cctx) {
        // No-op
    }

    /**
     * @param snapshotMsg Snapshot message.
     * @param initiator Initiator node.
     * @param msg message to log
     * @return Snapshot creation init future or {@code null} if snapshot is not available.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public IgniteInternalFuture startLocalSnapshotCreation(StartFullSnapshotAckDiscoveryMessage snapshotMsg,
        ClusterNode initiator, String msg)
        throws IgniteCheckedException {
        return null;
    }

    /**
     * @return Future that will be completed when indexes for given cache are restored.
     */
    @Nullable public IgniteInternalFuture indexRebuildFuture(int cacheId) {
        return null;
    }

    /**
     * See {@link GridCacheMapEntry#ensureFreeSpace()}
     *
     * @param memPlc Memory policy.
     */
    public void ensureFreeSpace(MemoryPolicy memPlc) throws IgniteCheckedException {
        if (memPlc == null)
            return;

        MemoryPolicyConfiguration plcCfg = memPlc.config();

        if (plcCfg.getPageEvictionMode() == DataPageEvictionMode.DISABLED)
            return;

        long memorySize = plcCfg.getMaxSize();

        PageMemory pageMem = memPlc.pageMemory();

        int sysPageSize = pageMem.systemPageSize();

        FreeListImpl freeListImpl = freeListMap.get(plcCfg.getName());

        for (;;) {
            long allocatedPagesCnt = pageMem.loadedPages();

            int emptyDataPagesCnt = freeListImpl.emptyDataPages();

            boolean shouldEvict = allocatedPagesCnt > (memorySize / sysPageSize * plcCfg.getEvictionThreshold()) &&
                emptyDataPagesCnt < plcCfg.getEmptyPagesPoolSize();

            if (shouldEvict)
                memPlc.evictionTracker().evictDataPage();
            else
                break;
        }
    }

    /**
     * @param memCfg memory configuration with common parameters.
     * @param plcCfg memory policy with PageMemory specific parameters.
     * @param memMetrics {@link MemoryMetrics} object to collect memory usage metrics.
     * @return Memory policy instance.
     */
    private MemoryPolicy initMemory(MemoryConfiguration memCfg, MemoryPolicyConfiguration plcCfg,
        MemoryMetricsImpl memMetrics) {
        File allocPath = buildAllocPath(plcCfg);

        DirectMemoryProvider memProvider = allocPath == null ?
            new UnsafeMemoryProvider(log) :
            new MappedFileMemoryProvider(
                log,
                allocPath);

        PageMemory pageMem = createPageMemory(memProvider, memCfg, plcCfg, memMetrics);

        return new MemoryPolicy(pageMem, plcCfg, memMetrics, createPageEvictionTracker(plcCfg, pageMem));
    }

    /**
     * @param plc Memory Policy Configuration.
     * @param pageMem Page memory.
     */
    private PageEvictionTracker createPageEvictionTracker(MemoryPolicyConfiguration plc, PageMemory pageMem) {
        if (plc.getPageEvictionMode() == DataPageEvictionMode.DISABLED)
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
     * @param plc MemoryPolicyConfiguration.
     */
    @Nullable protected File buildAllocPath(MemoryPolicyConfiguration plc) {
        String path = plc.getSwapFilePath();

        if (path == null)
            return null;

        String consId = String.valueOf(cctx.discovery().consistentId());

        consId = consId.replaceAll("[:,\\.]", "_");

        return buildPath(path, consId);
    }

    /**
     * Creates PageMemory with given size and memory provider.
     *
     * @param memProvider Memory provider.
     * @param memCfg Memory configuartion.
     * @param memPlcCfg Memory policy configuration.
     * @param memMetrics MemoryMetrics to collect memory usage metrics.
     * @return PageMemory instance.
     */
    protected PageMemory createPageMemory(
        DirectMemoryProvider memProvider,
        MemoryConfiguration memCfg,
        MemoryPolicyConfiguration memPlcCfg,
        MemoryMetricsImpl memMetrics
    ) {
        return new PageMemoryNoStoreImpl(log, memProvider, cctx, memCfg.getPageSize(), memPlcCfg, memMetrics, false);
    }

    /**
     * @param path Path to the working directory.
     * @param consId Consistent ID of the local node.
     * @return DB storage path.
     */
    protected File buildPath(String path, String consId) {
        String igniteHomeStr = U.getIgniteHome();

        File igniteHome = igniteHomeStr != null ? new File(igniteHomeStr) : null;

        File workDir = igniteHome == null ? new File(path) : new File(igniteHome, path);

        return new File(workDir, consId);
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) throws IgniteCheckedException {
        // No-op.
    }

    /**
     * @return Name of MemoryPolicyConfiguration for internal caches.
     */
    public String systemMemoryPolicyName() {
        return SYSTEM_MEMORY_POLICY_NAME;
    }
}
