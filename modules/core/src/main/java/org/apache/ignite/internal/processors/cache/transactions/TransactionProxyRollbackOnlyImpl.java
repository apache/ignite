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

package org.apache.ignite.internal.processors.cache.transactions;

import java.io.Externalizable;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Cache transaction proxy which supports only rollback or close operations and getters.
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
     * @param async Async.
     */
    public TransactionProxyRollbackOnlyImpl(GridNearTxLocal tx,
        GridCacheSharedContext<K, V> cctx, boolean async) {
        super(tx, cctx, async);
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
