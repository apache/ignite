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

import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.transactions.IgniteTxAlreadyCompletedCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxUnexpectedStateCheckedException;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.transactions.TransactionState;
import org.apache.ignite.transactions.TransactionUnsupportedConcurrencyException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Utils for MVCC.
 */
public class MvccUtils {
    /** Mask for operation counter bits. (Excludes hints and flags) */
    public static final int MVCC_OP_COUNTER_MASK = ~(Integer.MIN_VALUE >> 2);

    /** */
    public static final long MVCC_CRD_COUNTER_NA = 0L;

    /** */
    public static final long MVCC_COUNTER_NA = 0L;

    /** */
    public static final int MVCC_OP_COUNTER_NA = 0;

    /**
     *
     */
    private MvccUtils() {
    }

    /**
     * @param cctx Cache context.
     * @param mvccCrd Mvcc coordinator version.
     * @param mvccCntr Mvcc counter.
     * @param mvccOpCntr Mvcc operation counter.
     * @return TxState
     * @see TxState
     */
    public static byte state(GridCacheContext cctx, long mvccCrd, long mvccCntr, int mvccOpCntr) {
        return TxState.NA;
    }

    /**
     * @param grp Cache group context.
     * @param mvccCrd Mvcc coordinator version.
     * @param mvccCntr Mvcc counter.
     * @param mvccOpCntr Mvcc operation counter.
     * @return TxState
     * @see TxState
     */
    public static byte state(CacheGroupContext grp, long mvccCrd, long mvccCntr, int mvccOpCntr) {
        return TxState.NA;
    }

    /**
     *
     * @param grp Cache group context.
     * @param state State.
     * @param crd Mvcc coordinator counter.
     * @param cntr Mvcc counter.
     * @param opCntr Mvcc operation counter.
     * @return State exception.
     */
    public static IgniteTxUnexpectedStateCheckedException unexpectedStateException(
        CacheGroupContext grp, byte state, long crd, long cntr,
        int opCntr) {
        return unexpectedStateException(grp.shared().kernalContext(), state, crd, cntr, opCntr, null);
    }

    /**
     * @param cctx Cache context.
     * @param state State.
     * @param crd Mvcc coordinator counter.
     * @param cntr Mvcc counter.
     * @param opCntr Mvcc operation counter.
     * @param snapshot Mvcc snapshot
     * @return State exception.
     */
    public static IgniteTxUnexpectedStateCheckedException unexpectedStateException(
        GridCacheContext cctx, byte state, long crd, long cntr,
        int opCntr, MvccSnapshot snapshot) {
        return unexpectedStateException(cctx.kernalContext(), state, crd, cntr, opCntr, snapshot);
    }

    /** */
    private static IgniteTxUnexpectedStateCheckedException unexpectedStateException(GridKernalContext ctx, byte state, long crd, long cntr,
        int opCntr, MvccSnapshot snapshot) {
        String msg = "Unexpected state: [state=" + state + ", rowVer=" + crd + ":" + cntr + ":" + opCntr;

        msg += ", localNodeId=" + ctx.localNodeId() + "]";

        return new IgniteTxUnexpectedStateCheckedException(msg);
    }

    /**
     * Compares to pairs of coordinator/counter versions. See {@link Comparable}.
     *
     * @param mvccCrdLeft First coordinator version.
     * @param mvccCntrLeft First counter version.
     * @param mvccCrdRight Second coordinator version.
     * @param mvccCntrRight Second counter version.
     * @return Comparison result, see {@link Comparable}.
     */
    public static int compare(long mvccCrdLeft, long mvccCntrLeft, long mvccCrdRight, long mvccCntrRight) {
        return compare(mvccCrdLeft, mvccCntrLeft, 0, mvccCrdRight, mvccCntrRight, 0);
    }

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
            || (cmp = Integer.compare(mvccOpCntrLeft & MVCC_OP_COUNTER_MASK, mvccOpCntrRight & MVCC_OP_COUNTER_MASK)) != 0)
            return cmp;

        return 0;
    }

    /**
     * @param crdVer Mvcc coordinator version.
     * @param cntr Counter.
     * @param opCntr Operation counter.
     * @return Always {@code true}.
     */
    public static boolean mvccVersionIsValid(long crdVer, long cntr, int opCntr) {
        return mvccVersionIsValid(crdVer, cntr) && (opCntr & MVCC_OP_COUNTER_MASK) != MVCC_OP_COUNTER_NA;
    }

    /**
     * @param crdVer Mvcc coordinator version.
     * @param cntr Counter.
     * @return {@code True} if version is valid.
     */
    public static boolean mvccVersionIsValid(long crdVer, long cntr) {
        return crdVer > MVCC_CRD_COUNTER_NA && cntr > MVCC_COUNTER_NA;
    }

    /**
     * Checks transaction state.
     * @param tx Transaction.
     * @return Checked transaction.
     */
    public static GridNearTxLocal checkActive(GridNearTxLocal tx) throws IgniteTxAlreadyCompletedCheckedException {
        if (tx != null && tx.state() != TransactionState.ACTIVE)
            throw new IgniteTxAlreadyCompletedCheckedException("Transaction is already completed.");

        return tx;
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

    /**
     * @param ctx Grid kernal context.
     * @param timeout Transaction timeout.
     * @return Newly started SQL transaction.
     */
    public static GridNearTxLocal txStart(GridKernalContext ctx, long timeout) {
        return txStart(ctx, null, timeout);
    }

    /**
     * @param cctx Cache context.
     * @param timeout Transaction timeout.
     * @return Newly started SQL transaction.
     */
    public static GridNearTxLocal txStart(GridCacheContext cctx, long timeout) {
        return txStart(cctx.kernalContext(), cctx, timeout);
    }

    /**
     * @param ctx Grid kernal context.
     * @param cctx Cache context.
     * @param timeout Transaction timeout.
     * @return Newly started SQL transaction.
     */
    private static GridNearTxLocal txStart(GridKernalContext ctx, @Nullable GridCacheContext cctx, long timeout) {
        if (timeout == 0) {
            TransactionConfiguration tcfg = CU.transactionConfiguration(cctx, ctx.config());

            if (tcfg != null)
                timeout = tcfg.getDefaultTxTimeout();
        }

        GridNearTxLocal tx = ctx.cache().context().tm().newTx(
            false,
            false,
            cctx != null && cctx.systemTx() ? cctx : null,
            PESSIMISTIC,
            REPEATABLE_READ,
            timeout,
            cctx == null || !cctx.skipStore(),
            0,
            null
        );

        tx.syncMode(FULL_SYNC);

        return tx;
    }
}
