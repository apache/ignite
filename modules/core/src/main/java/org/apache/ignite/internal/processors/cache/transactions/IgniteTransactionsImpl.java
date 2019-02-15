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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collection;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteTransactionsEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionException;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionMetrics;
import org.jetbrains.annotations.Nullable;

/**
 * Grid transactions implementation.
 */
public class IgniteTransactionsImpl<K, V> implements IgniteTransactionsEx {
    /** Cache shared context. */
    private GridCacheSharedContext<K, V> cctx;

    /** Label. */
    private String lb;

    /**
     * @param cctx Cache shared context.
     * @param lb Label.
     */
    public IgniteTransactionsImpl(GridCacheSharedContext<K, V> cctx, @Nullable String lb) {
        this.cctx = cctx;
        this.lb = lb;
    }

    /** {@inheritDoc} */
    @Override public Transaction txStart() throws IllegalStateException {
        TransactionConfiguration cfg = CU.transactionConfiguration(null, cctx.kernalContext().config());

        return txStart0(
            cfg.getDefaultTxConcurrency(),
            cfg.getDefaultTxIsolation(),
            cfg.getDefaultTxTimeout(),
            0,
            null
        ).proxy();
    }

    /** {@inheritDoc} */
    @Override public Transaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        A.notNull(concurrency, "concurrency");
        A.notNull(isolation, "isolation");

        TransactionConfiguration cfg = CU.transactionConfiguration(null, cctx.kernalContext().config());

        return txStart0(
            concurrency,
            isolation,
            cfg.getDefaultTxTimeout(),
            0,
            null
        ).proxy();
    }

    /** {@inheritDoc} */
    @Override public Transaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation,
        long timeout, int txSize) {
        A.notNull(concurrency, "concurrency");
        A.notNull(isolation, "isolation");
        A.ensure(timeout >= 0, "timeout cannot be negative");
        A.ensure(txSize >= 0, "transaction size cannot be negative");

        return txStart0(
            concurrency,
            isolation,
            timeout,
            txSize,
            null
        ).proxy();
    }

    /** {@inheritDoc} */
    @Override public GridNearTxLocal txStartEx(
        GridCacheContext ctx,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        long timeout,
        int txSize)
    {
        A.notNull(concurrency, "concurrency");
        A.notNull(isolation, "isolation");
        A.ensure(timeout >= 0, "timeout cannot be negative");
        A.ensure(txSize >= 0, "transaction size cannot be negative");

        checkTransactional(ctx);

        return txStart0(concurrency,
            isolation,
            timeout,
            txSize,
            ctx.systemTx() ? ctx : null);
    }

    /** {@inheritDoc} */
    @Override public GridNearTxLocal txStartEx(
        GridCacheContext ctx,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation)
    {
        A.notNull(concurrency, "concurrency");
        A.notNull(isolation, "isolation");

        checkTransactional(ctx);

        TransactionConfiguration cfg = CU.transactionConfiguration(ctx, cctx.kernalContext().config());

        return txStart0(concurrency,
            isolation,
            cfg.getDefaultTxTimeout(),
            0,
            ctx.systemTx() ? ctx : null);
    }

    /**
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @param timeout Transaction timeout.
     * @param txSize Expected transaction size.
     * @param sysCacheCtx System cache context.
     * @return Transaction.
     */
    @SuppressWarnings("unchecked")
    private GridNearTxLocal txStart0(
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        long timeout,
        int txSize,
        @Nullable GridCacheContext sysCacheCtx
    ) {
        cctx.kernalContext().gateway().readLock();

        try {
            GridNearTxLocal tx = cctx.tm().userTx(sysCacheCtx);

            if (tx != null)
                throw new IllegalStateException("Failed to start new transaction " +
                    "(current thread already has a transaction): " + tx);

            tx = cctx.tm().newTx(
                false,
                false,
                sysCacheCtx,
                concurrency,
                isolation,
                timeout,
                true,
                null,
                txSize,
                lb
            );

            assert tx != null;

            return tx;
        }
        finally {
            cctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public Transaction tx() {
        GridNearTxLocal tx = cctx.tm().userTx();

        return tx != null ? tx.proxy() : null;
    }

    /** {@inheritDoc} */
    @Override public TransactionMetrics metrics() {
        return cctx.txMetrics();
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        cctx.resetTxMetrics();
    }

    /** {@inheritDoc} */
    @Override public Collection<Transaction> localActiveTransactions() {
        return F.viewReadOnly(cctx.tm().activeTransactions(), new IgniteClosure<IgniteInternalTx, Transaction>() {
            @Override public Transaction apply(IgniteInternalTx tx) {
                return ((GridNearTxLocal)tx).rollbackOnlyProxy();
            }
        }, new IgnitePredicate<IgniteInternalTx>() {
            @Override public boolean apply(IgniteInternalTx tx) {
                return tx.local() && tx.near();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteTransactions withLabel(String lb) {
        A.notNull(lb, "label should not be empty.");

        return new IgniteTransactionsImpl<>(cctx, lb);
    }

    /**
     * @param ctx Cache context.
     */
    private void checkTransactional(GridCacheContext ctx) {
        if (!ctx.transactional())
            throw new TransactionException("Failed to start transaction on non-transactional cache: " + ctx.name());
    }
}
