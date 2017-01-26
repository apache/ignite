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
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.MemoryConfiguration;
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
    /** */
    protected PageMemory pageMem;

    /** */
    private FreeListImpl freeList;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        if (!cctx.kernalContext().clientNode())
            init();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void init() throws IgniteCheckedException {
        if (pageMem == null) {
            MemoryConfiguration dbCfg = cctx.kernalContext().config().getMemoryConfiguration();

            if (dbCfg == null)
                dbCfg = new MemoryConfiguration();

            pageMem = initMemory(dbCfg);

            pageMem.start();

            initDataStructures();
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    protected void initDataStructures() throws IgniteCheckedException {
        freeList = new FreeListImpl(0, cctx.gridName(), pageMem, null, cctx.wal(), 0L, true);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void initDataBase() throws IgniteCheckedException{
        // No-op.
    }

    /**
     * @return Node-global free list.
     */
    public FreeList globalFreeList() {
        assert freeList != null : "Non initialized";

        return freeList;
    }

    /**
     * @return Node-global reuse list.
     */
    public ReuseList globalReuseList() {
        assert freeList != null : "Non initialized";

        return freeList;
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        if (pageMem != null)
            pageMem.stop();
    }

    /**
     *
     */
    public boolean persistenceEnabled() {
        return false;
    }

    /**
     * @return Page memory instance.
     */
    public PageMemory pageMemory() {
        return pageMem;
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
     * Reserve update history.
     *
     * @param cacheCtx Cache context.
     * @return Reserved update counters per partition.
     */
    public Map<Integer, Long> reserveHistory(GridCacheContext cacheCtx) throws IgniteCheckedException {
        return Collections.emptyMap();
    }

    /**
     * @param dbCfg Database configuration.
     * @return Page memory instance.
     */
    protected PageMemory initMemory(MemoryConfiguration dbCfg) {
        String path = dbCfg.getFileCacheAllocationPath();

        int concLvl = dbCfg.getConcurrencyLevel();

        if (concLvl < 1)
            concLvl = Runtime.getRuntime().availableProcessors();

        long fragmentSize = dbCfg.getPageCacheSize() / concLvl;

        if (fragmentSize < 1024 * 1024)
            fragmentSize = 1024 * 1024;

        String consId = String.valueOf(cctx.discovery().consistentId());

        consId = consId.replaceAll("[:,\\.]", "_");

        File allocPath = path == null ? null : buildPath(path, consId);

        long[] sizes = new long[concLvl];

        for (int i = 0; i < concLvl; i++)
            sizes[i] = fragmentSize;

        DirectMemoryProvider memProvider = path == null ?
            new UnsafeMemoryProvider(sizes) :
            new MappedFileMemoryProvider(
                log,
                allocPath,
                true,
                sizes);

        return new PageMemoryNoStoreImpl(log, memProvider, cctx, dbCfg.getPageSize());
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
}
