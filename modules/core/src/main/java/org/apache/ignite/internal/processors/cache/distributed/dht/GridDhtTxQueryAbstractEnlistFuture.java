/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.UUID;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheUpdateTxResult;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Abstract future processing transaction enlisting and locking of entries produced with DML and SELECT FOR UPDATE
 * queries.
 */
public abstract class GridDhtTxQueryAbstractEnlistFuture extends GridDhtTxAbstractEnlistFuture<Long> {
    /** Processed entries count. */
    protected long cnt;

    /**
     * Constructor.
     * @param nearNodeId Near node ID.
     * @param nearLockVer Near lock version.
     * @param mvccSnapshot Mvcc snapshot.
     * @param threadId Thread ID.
     * @param nearFutId Near future id.
     * @param nearMiniId Near mini future id.
     * @param tx Transaction.
     * @param timeout Lock acquisition timeout.
     * @param cctx Cache context.
     */
    protected GridDhtTxQueryAbstractEnlistFuture(UUID nearNodeId,
        GridCacheVersion nearLockVer,
        MvccSnapshot mvccSnapshot,
        long threadId,
        IgniteUuid nearFutId,
        int nearMiniId,
        GridDhtTxLocalAdapter tx,
        long timeout,
        GridCacheContext<?, ?> cctx) {
        super(nearNodeId,
            nearLockVer,
            mvccSnapshot,
            threadId,
            nearFutId,
            nearMiniId,
            tx,
            timeout,
            cctx,
            null,
            cctx.keepBinary());
    }

    /** {@inheritDoc} */
    @Override protected Long result0() {
        return cnt;
    }

    /** {@inheritDoc} */
    @Override protected void onEntryProcessed(KeyCacheObject key, GridCacheUpdateTxResult res) {
        if(res.success())
            cnt++;
    }
}
