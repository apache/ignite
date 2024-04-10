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

package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.transactions.TransactionUnsupportedConcurrencyException;
import org.jetbrains.annotations.Nullable;

/**
 * Utils for MVCC.
 */
public class MvccUtils {
    /**
     * Compares to pairs of coordinator/counter versions. See {@link Comparable}.
     *
     * @param mvccCrdLeft First coordinator version.
     * @param mvccCntrLeft First counter version.
     * @param mvccOpCntrLeft First operation counter.
     * @param mvccCrdRight Second coordinator version.
     * @param mvccCntrRight Second counter version.
     * @param mvccOpCntrRight Second operation counter.
     * @return Comparison result, see {@link Comparable}.
     */
    public static int compare(long mvccCrdLeft, long mvccCntrLeft, int mvccOpCntrLeft, long mvccCrdRight,
        long mvccCntrRight, int mvccOpCntrRight) {
        int cmp;

        if ((cmp = Long.compare(mvccCrdLeft, mvccCrdRight)) != 0
            || (cmp = Long.compare(mvccCntrLeft, mvccCntrRight)) != 0
            || (cmp = Integer.compare(mvccOpCntrLeft, mvccOpCntrRight)) != 0)
            return cmp;

        return 0;
    }

    /**
     * @param ctx Grid kernal context.
     * @return Currently started user transaction, or {@code null} if none started.
     * @throws TransactionUnsupportedConcurrencyException If transaction mode is not supported when MVCC is enabled.
     */
    @Nullable public static GridNearTxLocal tx(GridKernalContext ctx) {
        return tx(ctx, null);
    }

    /**
     * @param ctx Grid kernal context.
     * @param txId Transaction ID.
     * @return Currently started user transaction, or {@code null} if none started.
     * @throws TransactionUnsupportedConcurrencyException If transaction mode is not supported when MVCC is enabled.
     */
    @Nullable public static GridNearTxLocal tx(GridKernalContext ctx, @Nullable GridCacheVersion txId) {
        IgniteTxManager tm = ctx.cache().context().tm();

        IgniteInternalTx tx0 = txId == null ? tm.tx() : tm.tx(txId);

        GridNearTxLocal tx = tx0 != null && tx0.user() ? (GridNearTxLocal)tx0 : null;

        if (tx != null) {
            if (!tx.pessimistic()) {
                tx.setRollbackOnly();

                throw new TransactionUnsupportedConcurrencyException("Only pessimistic transactions are supported when MVCC is enabled.");
            }
        }

        return tx;
    }
}
