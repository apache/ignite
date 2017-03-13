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
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.file.MappedFileMemoryProvider;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.snapshot.StartFullSnapshotAckDiscoveryMessage;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.database.evict.PageEvictionTracker;
import org.apache.ignite.internal.processors.cache.database.evict.RandomLruPageEvictionTracker;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeListImpl;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IgniteCacheDatabaseSharedManager extends GridCacheSharedManagerAdapter implements IgniteChangeGlobalStateSupport {
    /** MemoryPolicyConfiguration name reserved for internal caches. */
    private static final String SYSTEM_MEMORY_POLICY_NAME = "sysMemPlc";

    /** Minimum size of memory chunk */
    private static final long MIN_PAGE_MEMORY_SIZE = 1024 * 1024;

    /** */
    protected Map<String, MemoryPolicy> memPlcMap;

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
        if (!cctx.kernalContext().clientNode())
            init();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void init() throws IgniteCheckedException {
        if (memPlcMap == null) {
            MemoryConfiguration dbCfg = cctx.kernalContext().config().getMemoryConfiguration();

            if (dbCfg == null) {
                dbCfg = new MemoryConfiguration();
                dbCfg.setMemoryPolicies(dbCfg.createDefaultPolicy());
            }
            else
                validateConfiguration(dbCfg);

            pageSize = dbCfg.getPageSize();

            initPageMemoryPools(dbCfg);

            startPageMemoryPools();

            initPageMemoryDataStructures(dbCfg);
        }
    }

    /**
     * @param dbCfg Database config.
     */
    protected void initPageMemoryDataStructures(MemoryConfiguration dbCfg) throws IgniteCheckedException {
        freeListMap = U.newHashMap(memPlcMap.size());

        for (MemoryPolicyConfiguration memPlc : dbCfg.getMemoryPolicies()) {
            String plcName = memPlc.getName();

            FreeListImpl freeList = new FreeListImpl(0, cctx.gridName(), pageMemory(plcName), null, cctx.wal(), 0L, true);

            freeListMap.put(plcName, freeList);

            if (memPlc.isDefault())
                dfltFreeList = freeList;
        }

        freeListMap.put(SYSTEM_MEMORY_POLICY_NAME,
                new FreeListImpl(0,
                        cctx.gridName(),
                        pageMemory(SYSTEM_MEMORY_POLICY_NAME),
                        null,
                        cctx.wal(),
                        0L,
                        true));
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
    private void startPageMemoryPools() {
        for (MemoryPolicy memPlc : memPlcMap.values())
            memPlc.pageMemory().start();
    }

    /**
     * @param dbCfg Database config.
     */
    protected void initPageMemoryPools(MemoryConfiguration dbCfg) {
        MemoryPolicyConfiguration[] memPlcs = dbCfg.getMemoryPolicies();

        memPlcMap = U.newHashMap(memPlcs.length + 1);

        for (MemoryPolicyConfiguration memPlcCfg : memPlcs) {
            MemoryPolicy memPlc = initMemory(dbCfg, memPlcCfg);

            memPlcMap.put(memPlcCfg.getName(), memPlc);

            if (memPlcCfg.isDefault())
                dfltMemPlc = memPlc;
        }

        MemoryPolicyConfiguration sysPlcCfg = createSystemMemoryPolicy(dbCfg.getSystemCacheMemorySize());

        memPlcMap.put(SYSTEM_MEMORY_POLICY_NAME, initMemory(dbCfg, sysPlcCfg));
    }

    /**
     * @param sysCacheMemSize size of PageMemory to be created for system cache.
     */
    private MemoryPolicyConfiguration createSystemMemoryPolicy(long sysCacheMemSize) {
        MemoryPolicyConfiguration res = new MemoryPolicyConfiguration();

        res.setName(SYSTEM_MEMORY_POLICY_NAME);
        res.setSize(sysCacheMemSize);

        return res;
    }

    /**
     * @param dbCfg configuration to validate.
     */
    private void validateConfiguration(MemoryConfiguration dbCfg) throws IgniteCheckedException {
        MemoryPolicyConfiguration[] plcCfgs = dbCfg.getMemoryPolicies();

        if (plcCfgs == null) {
            plcCfgs = new MemoryPolicyConfiguration[1];

            plcCfgs[0] = dbCfg.createDefaultPolicy();

            dbCfg.setMemoryPolicies(plcCfgs);
        }

        boolean dfltPlcPresented = false;
        Set<String> plcsNames = U.newHashSet(plcCfgs.length);

        for (MemoryPolicyConfiguration plcCfg : plcCfgs) {
            assert plcCfg != null;

            if (dfltPlcPresented) {
                if (plcCfg.isDefault())
                    throw new IgniteCheckedException("Only one default MemoryPolicyConfiguration must be presented.");
            }
            else
                dfltPlcPresented = plcCfg.isDefault();

            checkPolicyName(plcCfg, plcsNames);

            checkPolicySize(plcCfg);
        }

        if (!dfltPlcPresented)
            throw new IgniteCheckedException("One default MemoryPolicyConfiguration must be presented.");
    }

    /**
     * @param plcCfg MemoryPolicyConfiguration to validate.
     */
    private static void checkPolicySize(MemoryPolicyConfiguration plcCfg) throws IgniteCheckedException {
        if (plcCfg.getSize() < MIN_PAGE_MEMORY_SIZE)
            throw new IgniteCheckedException("MemoryPolicy must have size more than 1MB: " + plcCfg.getName());
    }

    /**
     * @param plcCfg MemoryPolicyConfiguration to validate.
     * @param observedNames names of MemoryPolicies observed before.
     */
    private static void checkPolicyName(MemoryPolicyConfiguration plcCfg, Set<String> observedNames) throws IgniteCheckedException {
        String name = plcCfg.getName();

        if (plcCfg.isDefault() && name != null)
            throw new IgniteCheckedException("Default MemoryPolicyConfiguration must have a null name.");

        if (!plcCfg.isDefault() && (name == null || name.isEmpty()))
            throw new IgniteCheckedException("Non-default MemoryPolicyConfiguration must have non-null name.");

        if (observedNames.contains(name))
            throw new IgniteCheckedException("Two MemoryPolicies have the same name: " + name);

        if (SYSTEM_MEMORY_POLICY_NAME.equals(name))
            throw new IgniteCheckedException("'sysMemPlc' policy name is reserved for internal use.");

        observedNames.add(name);
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
     * @param memPlcName MemoryPolicyConfiguration name.
     * @return {@link PageMemory} instance associated with a given {@link MemoryPolicyConfiguration}.
     */
    public PageMemory pageMemory(String memPlcName) {
        MemoryPolicy memPlc = memoryPolicy(memPlcName);

        return memPlc == null ? null : memPlc.pageMemory();
    }

    /**
     * @param memPlcName Memory policy name.
     * @return {@link MemoryPolicy} instance associated with a given {@link MemoryPolicyConfiguration}.
     */
    public MemoryPolicy memoryPolicy(String memPlcName) {
        if (memPlcName == null)
            return dfltMemPlc;

        return memPlcMap != null ? memPlcMap.get(memPlcName) : null;
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
            for (MemoryPolicy memPlc : memPlcMap.values())
                memPlc.pageMemory().stop();
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
     * @param dbCfg memory configuration with common parameters.
     * @param plc memory policy with PageMemory specific parameters.
     * @return Memory policy instance.
     */
    private MemoryPolicy initMemory(MemoryConfiguration dbCfg, MemoryPolicyConfiguration plc) {
        long[] sizes = calculateFragmentSizes(
                dbCfg.getConcurrencyLevel(),
                plc.getSize());

        File allocPath = buildAllocPath(plc);

        DirectMemoryProvider memProvider = allocPath == null ?
            new UnsafeMemoryProvider(sizes) :
            new MappedFileMemoryProvider(
                log,
                allocPath,
                true,
                sizes);

        PageMemory pageMem = createPageMemory(memProvider, dbCfg.getPageSize());

        return new MemoryPolicy(pageMem, plc, new RandomLruPageEvictionTracker(pageMem, plc, cctx));

    }

    /**
     * Calculate fragment sizes for a cache with given size and concurrency level.
     * @param concLvl Concurrency level.
     * @param cacheSize Cache size.
     */
    protected long[] calculateFragmentSizes(int concLvl, long cacheSize) {
        if (concLvl < 1)
            concLvl = Runtime.getRuntime().availableProcessors();

        long fragmentSize = cacheSize / concLvl;

        if (fragmentSize < 1024 * 1024)
            fragmentSize = 1024 * 1024;

        long[] sizes = new long[concLvl];

        for (int i = 0; i < concLvl; i++)
            sizes[i] = fragmentSize;

        return sizes;
    }

    /**
     * Builds allocation path for memory mapped file to be used with PageMemory.
     *
     * @param plc MemoryPolicyConfiguration.
     */
    @Nullable protected File buildAllocPath(MemoryPolicyConfiguration plc) {
        String path = plc.getTmpFsPath();

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
     * @param pageSize Page size.
     */
    protected PageMemory createPageMemory(DirectMemoryProvider memProvider, int pageSize) {
        return new PageMemoryNoStoreImpl(log, memProvider, cctx, pageSize, false);
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

    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) throws IgniteCheckedException {

    }

    /**
     * @return Name of MemoryPolicyConfiguration for internal caches.
     */
    public String systemMemoryPolicyName() {
        return SYSTEM_MEMORY_POLICY_NAME;
    }
}
