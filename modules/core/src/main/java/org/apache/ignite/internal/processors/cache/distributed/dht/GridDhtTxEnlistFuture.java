/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
     * @param keepBinary Keep binary flag.
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
        boolean needRes,
        boolean keepBinary) {
        super(nearNodeId,
            nearLockVer,
            mvccSnapshot,
            threadId,
            nearFutId,
            nearMiniId,
            tx,
            timeout,
            cctx,
            filter,
            keepBinary);

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

        if(txRes.invokeResult() != null) {
            res.invokeResult(true);

            CacheInvokeResult invokeRes = txRes.invokeResult();

            if (invokeRes.result() != null || invokeRes.error() != null)
                res.addEntryProcessResult(cctx, key, null, invokeRes.result(), invokeRes.error(), keepBinary);
        }
        else if (needRes)
            res.set(cctx, txRes.prevValue(), txRes.success(), keepBinary);
    }

    /** {@inheritDoc} */
    @Override public boolean needResult() {
        return needRes;
    }

    /** {@inheritDoc} */
    @Override public EnlistOperation operation() {
        return op;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNextX() {
        return it.hasNext();
    }

    /** {@inheritDoc} */
    @Override public Object nextX() {
        return it.next();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxEnlistFuture.class, this);
    }
}
