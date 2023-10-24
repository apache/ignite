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

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.PartitionsEvictManager;
import org.apache.ignite.internal.processors.cache.jta.CacheJtaManagerAdapter;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCachingManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;

/** Builder for {@link GridCacheSharedContext}. */
public class GridCacheSharedContextBuilder {
    /** */
    private IgniteTxManager txMgr;

    /** */
    private CacheJtaManagerAdapter jtaMgr;

    /** */
    private GridCacheVersionManager verMgr;

    /** */
    private GridCacheMvccManager mvccMgr;

    /** */
    private IgnitePageStoreManager pageStoreMgr;

    /** */
    private IgniteWriteAheadLogManager walMgr;

    /** */
    private WalStateManager walStateMgr;

    /** */
    private IgniteCacheDatabaseSharedManager dbMgr;

    /** */
    private IgniteSnapshotManager snapshotMgr;

    /** */
    private GridCacheDeploymentManager<?, ?> depMgr;

    /** */
    private GridCachePartitionExchangeManager<?, ?> exchMgr;

    /** */
    private CacheAffinitySharedManager<?, ?> affMgr;

    /** */
    private GridCacheIoManager ioMgr;

    /** */
    private GridCacheSharedTtlCleanupManager ttlMgr;

    /** */
    private PartitionsEvictManager evictMgr;

    /** */
    private MvccCachingManager mvccCachingMgr;

    /** */
    private CacheDiagnosticManager diagnosticMgr;

    /** */
    public <K, V> GridCacheSharedContext<K, V> build(
        GridKernalContext kernalCtx,
        Collection<CacheStoreSessionListener> storeSesLsnrs
    ) {
        return new GridCacheSharedContext(
            kernalCtx,
            txMgr,
            verMgr,
            mvccMgr,
            pageStoreMgr,
            walMgr,
            walStateMgr,
            dbMgr,
            snapshotMgr,
            depMgr,
            exchMgr,
            affMgr,
            ioMgr,
            ttlMgr,
            evictMgr,
            jtaMgr,
            storeSesLsnrs,
            mvccCachingMgr,
            diagnosticMgr
        );
    }

    /** */
    public GridCacheSharedContextBuilder setTxManager(IgniteTxManager txMgr) {
        this.txMgr = txMgr;

        return this;
    }

    /** */
    public GridCacheSharedContextBuilder setJtaManager(CacheJtaManagerAdapter jtaMgr) {
        this.jtaMgr = jtaMgr;

        return this;
    }

    /** */
    public GridCacheSharedContextBuilder setVersionManager(GridCacheVersionManager verMgr) {
        this.verMgr = verMgr;

        return this;
    }

    /** */
    public GridCacheSharedContextBuilder setMvccManager(GridCacheMvccManager mvccMgr) {
        this.mvccMgr = mvccMgr;

        return this;
    }

    /** */
    public GridCacheSharedContextBuilder setPageStoreManager(IgnitePageStoreManager pageStoreMgr) {
        this.pageStoreMgr = pageStoreMgr;

        return this;
    }

    /** */
    public GridCacheSharedContextBuilder setWalManager(IgniteWriteAheadLogManager walMgr) {
        this.walMgr = walMgr;

        return this;
    }

    /** */
    public GridCacheSharedContextBuilder setWalStateManager(WalStateManager walStateMgr) {
        this.walStateMgr = walStateMgr;

        return this;
    }

    /** */
    public GridCacheSharedContextBuilder setDatabaseManager(IgniteCacheDatabaseSharedManager dbMgr) {
        this.dbMgr = dbMgr;

        return this;
    }

    /** */
    public GridCacheSharedContextBuilder setSnapshotManager(IgniteSnapshotManager snapshotMgr) {
        this.snapshotMgr = snapshotMgr;

        return this;
    }

    /** */
    public GridCacheSharedContextBuilder setDeploymentManager(GridCacheDeploymentManager depMgr) {
        this.depMgr = depMgr;

        return this;
    }

    /** */
    public GridCacheSharedContextBuilder setPartitionExchangeManager(GridCachePartitionExchangeManager exchMgr) {
        this.exchMgr = exchMgr;

        return this;
    }

    /** */
    public GridCacheSharedContextBuilder setAffinityManager(CacheAffinitySharedManager affMgr) {
        this.affMgr = affMgr;

        return this;
    }

    /** */
    public GridCacheSharedContextBuilder setIoManager(GridCacheIoManager ioMgr) {
        this.ioMgr = ioMgr;

        return this;
    }

    /** */
    public GridCacheSharedContextBuilder setTtlCleanupManager(GridCacheSharedTtlCleanupManager ttlMgr) {
        this.ttlMgr = ttlMgr;

        return this;
    }

    /** */
    public GridCacheSharedContextBuilder setPartitionsEvictManager(PartitionsEvictManager evictMgr) {
        this.evictMgr = evictMgr;

        return this;
    }

    /** */
    public GridCacheSharedContextBuilder setMvccCachingManager(MvccCachingManager mvccCachingMgr) {
        this.mvccCachingMgr = mvccCachingMgr;

        return this;
    }

    /** */
    public GridCacheSharedContextBuilder setDiagnosticManager(CacheDiagnosticManager diagnosticMgr) {
        this.diagnosticMgr = diagnosticMgr;

        return this;
    }
}
