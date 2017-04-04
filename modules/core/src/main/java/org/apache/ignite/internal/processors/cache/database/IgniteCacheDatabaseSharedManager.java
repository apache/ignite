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
import java.util.HashSet;
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

            if (dbCfg == null)
                dbCfg = new MemoryConfiguration();

            validateConfiguration(dbCfg);

            pageSize = dbCfg.getPageSize();

            initPageMemoryPolicies(dbCfg);

            startPageMemoryPools();

            initPageMemoryDataStructures(dbCfg);
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

            FreeListImpl freeList = new FreeListImpl(0,
                    cctx.igniteInstanceName(),
                    memPlc.pageMemory(),
                    null,
                    cctx.wal(),
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
    private void startPageMemoryPools() {
        for (MemoryPolicy memPlc : memPlcMap.values())
            memPlc.pageMemory().start();
    }

    /**
     * @param dbCfg Database config.
     */
    protected void initPageMemoryPolicies(MemoryConfiguration dbCfg) {
        MemoryPolicyConfiguration[] memPlcsCfgs = dbCfg.getMemoryPolicies();

        if (memPlcsCfgs == null) {
            //reserve place for default and system memory policies
            memPlcMap = U.newHashMap(2);

            dfltMemPlc = createDefaultMemoryPolicy(dbCfg);
            memPlcMap.put(null, dfltMemPlc);

            log.warning("No user-defined default MemoryPolicy found; system default of 1GB size will be used.");
        }
        else {
            String dfltMemPlcName = dbCfg.getDefaultMemoryPolicyName();

            if (dfltMemPlcName == null) {
                //reserve additional place for default and system memory policies
                memPlcMap = U.newHashMap(memPlcsCfgs.length + 2);

                dfltMemPlc = createDefaultMemoryPolicy(dbCfg);
                memPlcMap.put(null, dfltMemPlc);

                log.warning("No user-defined default MemoryPolicy found; system default of 1GB size will be used.");
            }
            else
                //reserve additional place for system memory policy only
                memPlcMap = U.newHashMap(memPlcsCfgs.length + 1);

            for (MemoryPolicyConfiguration memPlcCfg : memPlcsCfgs) {
                PageMemory pageMem = initMemory(dbCfg, memPlcCfg);

                MemoryPolicy memPlc = new MemoryPolicy(pageMem, memPlcCfg);

                memPlcMap.put(memPlcCfg.getName(), memPlc);

                if (memPlcCfg.getName().equals(dfltMemPlcName))
                    dfltMemPlc = memPlc;
            }
        }

        MemoryPolicyConfiguration sysPlcCfg = createSystemMemoryPolicy(dbCfg.getSystemCacheMemorySize());

        memPlcMap.put(SYSTEM_MEMORY_POLICY_NAME, new MemoryPolicy(initMemory(dbCfg, sysPlcCfg), sysPlcCfg));
    }

    /**
     * @param dbCfg Database config.
     */
    private MemoryPolicy createDefaultMemoryPolicy(MemoryConfiguration dbCfg) {
        MemoryPolicyConfiguration dfltPlc = dbCfg.createDefaultPolicyConfig();

        PageMemory pageMem = initMemory(dbCfg, dfltPlc);

        return new MemoryPolicy(pageMem, dfltPlc);
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

        Set<String> plcNames = (plcCfgs != null) ? U.<String>newHashSet(plcCfgs.length) : new HashSet<String>(0);

        if (plcCfgs != null) {
            for (MemoryPolicyConfiguration plcCfg : plcCfgs) {
                assert plcCfg != null;

                checkPolicyName(plcCfg.getName(), plcNames);

                checkPolicySize(plcCfg);
            }
        }

        checkDefaultPolicyConfiguration(dbCfg.getDefaultMemoryPolicyName(), plcNames);
    }

    /**
     * @param dfltPlcName Default MemoryPolicy name.
     * @param plcNames All MemoryPolicy names.
     * @throws IgniteCheckedException In case of validation violation.
     */
    private static void checkDefaultPolicyConfiguration(String dfltPlcName, Set<String> plcNames) throws IgniteCheckedException {
        if (dfltPlcName != null) {
            if (dfltPlcName.isEmpty())
                throw new IgniteCheckedException("User-defined default MemoryPolicy name must be non-empty");
            if (!plcNames.contains(dfltPlcName))
                throw new IgniteCheckedException("User-defined default MemoryPolicy name must be presented among configured MemoryPolices: " + dfltPlcName);
        }
    }

    /**
     * @param plcCfg MemoryPolicyConfiguration to validate.
     */
    private static void checkPolicySize(MemoryPolicyConfiguration plcCfg) throws IgniteCheckedException {
        if (plcCfg.getSize() < MIN_PAGE_MEMORY_SIZE)
            throw new IgniteCheckedException("MemoryPolicy must have size more than 1MB: " + plcCfg.getName());
    }

    /**
     * @param plcName MemoryPolicy name to validate.
     * @param observedNames Names of MemoryPolicies observed before.
     */
    private static void checkPolicyName(String plcName, Set<String> observedNames) throws IgniteCheckedException {
        if (plcName == null || plcName.isEmpty())
            throw new IgniteCheckedException("User-defined MemoryPolicyConfiguration must have non-null and non-empty name.");

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
     * @return Page memory instance.
     */
    private PageMemory initMemory(MemoryConfiguration dbCfg, MemoryPolicyConfiguration plc) {
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

        return createPageMemory(memProvider, dbCfg.getPageSize());
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
