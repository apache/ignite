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

package org.apache.ignite.internal.processors.cache.transactions;

import java.io.Externalizable;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Cache transaction proxy which support only rollback or close operations and getters.
 */
public class TransactionProxyRollbackOnlyImpl<K, V> extends TransactionProxyImpl<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public TransactionProxyRollbackOnlyImpl() {
        // No-op.
    }

    /**
     * @param tx Tx.
     * @param cctx Cctx.
     */
    public TransactionProxyRollbackOnlyImpl(GridNearTxLocal tx,
        GridCacheSharedContext<K, V> cctx) {
        super(tx, cctx);
    }

    /** {@inheritDoc} */
    @Override public boolean setRollbackOnly() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void commit() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> commitAsync() throws IgniteException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void suspend() throws IgniteException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void resume() throws IgniteException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public long timeout(long timeout) {
        throw new UnsupportedOperationException();
    }
}
