/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheInvokeResult;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheUpdateTxResult;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.EnlistOperation;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Future processing transaction enlisting and locking of entries produces by cache API operations.
 */
public final class GridDhtTxEnlistFuture extends GridDhtTxAbstractEnlistFuture<GridCacheReturn> implements UpdateSourceIterator<Object> {
    /** Enlist operation. */
    private EnlistOperation op;

    /** Source iterator. */
    private Iterator<Object> it;

    /** Future result. */
    private GridCacheReturn res;

    /** Need result flag. If {@code True} previous value should be returned as well. */
    private boolean needRes;

    /**
     * Constructor.
     *
     * @param nearNodeId Near node ID.
     * @param nearLockVer Near lock version.
     * @param mvccSnapshot Mvcc snapshot.
     * @param threadId Thread ID.
     * @param nearFutId Near future id.
     * @param nearMiniId Near mini future id.
     * @param tx Transaction.
     * @param timeout Lock acquisition timeout.
     * @param cctx Cache context.
     * @param rows Collection of rows.
     * @param op Operation.
     * @param filter Filter.
     * @param needRes Return previous value flag.
     */
    public GridDhtTxEnlistFuture(UUID nearNodeId,
        GridCacheVersion nearLockVer,
        MvccSnapshot mvccSnapshot,
        long threadId,
        IgniteUuid nearFutId,
        int nearMiniId,
        GridDhtTxLocalAdapter tx,
        long timeout,
        GridCacheContext<?, ?> cctx,
        Collection<Object> rows,
        EnlistOperation op,
        @Nullable CacheEntryPredicate filter,
        boolean needRes) {
        super(nearNodeId,
            nearLockVer,
            mvccSnapshot,
            threadId,
            nearFutId,
            nearMiniId,
            null,
            tx,
            timeout,
            cctx,
            filter);

        this.op = op;
        this.needRes = needRes;

        it = rows.iterator();

        res = new GridCacheReturn(cctx.localNodeId().equals(nearNodeId), false);

        skipNearNodeUpdates = true;
    }

    /** {@inheritDoc} */
    @Override protected UpdateSourceIterator<?> createIterator() throws IgniteCheckedException {
        return this;
    }

    /** {@inheritDoc} */
    @Override @Nullable protected GridCacheReturn result0() {
        return res;
    }

    /** {@inheritDoc} */
    @Override protected void onEntryProcessed(KeyCacheObject key, GridCacheUpdateTxResult txRes) {
        assert txRes.invokeResult() == null || needRes;

        res.success(txRes.success());

        if(txRes.invokeResult() != null)
            res.invokeResult(true);

        if (needRes && txRes.success()) {
            CacheInvokeResult invokeRes = txRes.invokeResult();

            if (invokeRes != null) {
                if(invokeRes.result() != null || invokeRes.error() != null)
                    res.addEntryProcessResult(cctx, key, null, invokeRes.result(), invokeRes.error(), cctx.keepBinary());
            }
            else
                res.set(cctx, txRes.prevValue(), txRes.success(), true);
        }
    }

    /** {@inheritDoc} */
    public boolean needResult() {
        return needRes;
    }

    /** {@inheritDoc} */
    @Override public EnlistOperation operation() {
        return op;
    }

    /** {@inheritDoc} */
    public boolean hasNextX() {
        return it.hasNext();
    }

    /** {@inheritDoc} */
    public Object nextX() {
        return it.next();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxEnlistFuture.class, this);
    }
}
