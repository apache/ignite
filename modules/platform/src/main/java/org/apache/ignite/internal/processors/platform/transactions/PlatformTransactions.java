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

package org.apache.ignite.internal.processors.platform.transactions;

import java.util.Date;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.portable.PortableRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.utils.PlatformFutureUtils;
import org.apache.ignite.internal.util.GridConcurrentFactory;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionMetrics;

/**
 * Native transaction wrapper implementation.
 */
@SuppressWarnings({"unchecked", "UnusedDeclaration", "TryFinallyCanBeTryWithResources"})
public class PlatformTransactions extends PlatformAbstractTarget {
    /** */
    public static final int OP_CACHE_CONFIG_PARAMETERS = 1;

    /** */
    public static final int OP_METRICS = 2;

    /** */
    private final IgniteTransactions txs;

    /** Map with currently active transactions. */
    private final ConcurrentMap<Long, Transaction> txMap = GridConcurrentFactory.newMap();

    /** Transaction ID sequence. Must be static to ensure uniqueness across different caches. */
    private static final AtomicLong TX_ID_GEN = new AtomicLong();

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     */
    public PlatformTransactions(PlatformContext platformCtx) {
        super(platformCtx);

        txs = platformCtx.kernalContext().grid().transactions();
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout
     * @param txSize Number of entries participating in transaction.
     * @return Transaction thread ID.
     */
    public long txStart(int concurrency, int isolation, long timeout, int txSize) {
        TransactionConcurrency txConcurrency = TransactionConcurrency.fromOrdinal(concurrency);

        assert txConcurrency != null;

        TransactionIsolation txIsolation = TransactionIsolation.fromOrdinal(isolation);

        assert txIsolation != null;

        Transaction tx = txs.txStart(txConcurrency, txIsolation);

        return registerTx(tx);
    }

    /**
     * @param id Transaction ID.
     * @throws org.apache.ignite.IgniteCheckedException In case of error.
     */
    public int txCommit(long id) throws IgniteCheckedException {
        tx(id).commit();

        return txClose(id);
    }

    /**
     * @param id Transaction ID.
     * @throws org.apache.ignite.IgniteCheckedException In case of error.
     */
    public int txRollback(long id) throws IgniteCheckedException {
        tx(id).rollback();

        return txClose(id);
    }

    /**
     * @param id Transaction ID.
     * @throws org.apache.ignite.IgniteCheckedException In case of error.
     * @return Transaction state.
     */
    public int txClose(long id) throws IgniteCheckedException {
        Transaction tx = tx(id);

        try {
            tx.close();

            return tx.state().ordinal();
        }
        finally {
            unregisterTx(id);
        }
    }

    /**
     * @param id Transaction ID.
     * @return Transaction state.
     */
    public int txState(long id) {
        Transaction tx = tx(id);

        return tx.state().ordinal();
    }

    /**
     * @param id Transaction ID.
     * @return {@code True} if rollback only flag was set.
     */
    public boolean txSetRollbackOnly(long id) {
        Transaction tx = tx(id);

        return tx.setRollbackOnly();
    }

    /**
     * Commits tx in async mode.
     */
    public void txCommitAsync(final long txId, final long futId) {
        final Transaction asyncTx = (Transaction)tx(txId).withAsync();

        asyncTx.commit();

        listenAndNotifyIntFuture(futId, asyncTx);
    }

    /**
     * Rolls back tx in async mode.
     */
    public void txRollbackAsync(final long txId, final long futId) {
        final Transaction asyncTx = (Transaction)tx(txId).withAsync();

        asyncTx.rollback();

        listenAndNotifyIntFuture(futId, asyncTx);
    }

    /**
     * Listens to the transaction future and notifies .NET int future.
     */
    private void listenAndNotifyIntFuture(final long futId, final Transaction asyncTx) {
        IgniteFuture fut = asyncTx.future().chain(new C1<IgniteFuture, Object>() {
            private static final long serialVersionUID = 0L;

            @Override public Object apply(IgniteFuture fut) {
                return null;
            }
        });

        PlatformFutureUtils.listen(platformCtx, fut, futId, PlatformFutureUtils.TYP_OBJ, this);
    }

    /**
     * Resets transaction metrics.
     */
    public void resetMetrics() {
       txs.resetMetrics();
    }

    /**
     * Register transaction.
     *
     * @param tx Transaction.
     * @return Transaction ID.
     */
    private long registerTx(Transaction tx) {
        long id = TX_ID_GEN.incrementAndGet();

        Transaction old = txMap.put(id, tx);

        assert old == null : "Duplicate TX ids: " + old;

        return id;
    }

    /**
     * Unregister transaction.
     *
     * @param id Transaction ID.
     */
    private void unregisterTx(long id) {
        Transaction tx = txMap.remove(id);

        assert tx != null : "Failed to unregister transaction: " + id;
    }

    /**
     * Get transaction by ID.
     *
     * @param id ID.
     * @return Transaction.
     */
    private Transaction tx(long id) {
        Transaction tx = txMap.get(id);

        assert tx != null : "Transaction not found for ID: " + id;

        return tx;
    }

    /** {@inheritDoc} */
    @Override protected void processOutStream(int type, PortableRawWriterEx writer) throws IgniteCheckedException {
        switch (type) {
            case OP_CACHE_CONFIG_PARAMETERS:
                TransactionConfiguration txCfg = platformCtx.kernalContext().config().getTransactionConfiguration();

                writer.writeEnum(txCfg.getDefaultTxConcurrency());
                writer.writeEnum(txCfg.getDefaultTxIsolation());
                writer.writeLong(txCfg.getDefaultTxTimeout());

                break;

            case OP_METRICS:
                TransactionMetrics metrics = txs.metrics();

                writer.writeDate(new Date(metrics.commitTime()));
                writer.writeDate(new Date(metrics.rollbackTime()));
                writer.writeInt(metrics.txCommits());
                writer.writeInt(metrics.txRollbacks());

                break;

            default:
                super.processOutStream(type, writer);
        }
    }
}