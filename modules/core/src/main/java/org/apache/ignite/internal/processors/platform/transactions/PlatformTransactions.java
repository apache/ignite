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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.util.GridConcurrentFactory;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionMetrics;

import java.sql.Timestamp;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

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
    public static final int OP_START = 3;

    /** */
    public static final int OP_COMMIT = 4;

    /** */
    public static final int OP_ROLLBACK = 5;

    /** */
    public static final int OP_CLOSE = 6;

    /** */
    public static final int OP_STATE = 7;

    /** */
    public static final int OP_SET_ROLLBACK_ONLY = 8;

    /** */
    public static final int OP_COMMIT_ASYNC = 9;

    /** */
    public static final int OP_ROLLBACK_ASYNC = 10;

    /** */
    public static final int OP_RESET_METRICS = 11;

    /** */
    public static final int OP_PREPARE = 12;

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
     * @param id Transaction ID.
     * @return Transaction state.
     */
    private int txClose(long id) {
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
    @Override public long processInLongOutLong(int type, long val) throws IgniteCheckedException {
        switch (type) {
            case OP_PREPARE:
                ((TransactionProxyImpl)tx(val)).tx().prepare();

                return TRUE;

            case OP_COMMIT:
                tx(val).commit();

                return txClose(val);

            case OP_ROLLBACK:
                tx(val).rollback();

                return txClose(val);

            case OP_CLOSE:
                return txClose(val);

            case OP_SET_ROLLBACK_ONLY:
                return tx(val).setRollbackOnly() ? TRUE : FALSE;

            case OP_STATE:
                return tx(val).state().ordinal();

            case OP_RESET_METRICS:
                txs.resetMetrics();

                return TRUE;
        }

        return super.processInLongOutLong(type, val);
    }

    /** {@inheritDoc} */
    @Override public long processInStreamOutLong(int type, BinaryRawReaderEx reader) throws IgniteCheckedException {
        long txId = reader.readLong();

        final Transaction asyncTx = (Transaction)tx(txId).withAsync();

        switch (type) {
            case OP_COMMIT_ASYNC:
                asyncTx.commit();

                break;


            case OP_ROLLBACK_ASYNC:
                asyncTx.rollback();

                break;

            default:
                return super.processInStreamOutLong(type, reader);
        }

        // Future result is the tx itself, we do not want to return it to the platform.
        IgniteFuture fut = asyncTx.future().chain(new C1<IgniteFuture, Object>() {
            private static final long serialVersionUID = 0L;

            @Override public Object apply(IgniteFuture fut) {
                return null;
            }
        });

        readAndListenFuture(reader, fut);

        return TRUE;
    }

    /** {@inheritDoc} */
    @Override public void processInStreamOutStream(int type, BinaryRawReaderEx reader, BinaryRawWriterEx writer) throws IgniteCheckedException {
        switch (type) {
            case OP_START: {
                TransactionConcurrency txConcurrency = TransactionConcurrency.fromOrdinal(reader.readInt());

                assert txConcurrency != null;

                TransactionIsolation txIsolation = TransactionIsolation.fromOrdinal(reader.readInt());

                assert txIsolation != null;

                Transaction tx = txs.txStart(txConcurrency, txIsolation, reader.readLong(), reader.readInt());

                long id = registerTx(tx);

                writer.writeLong(id);

                return;
            }
        }

        super.processInStreamOutStream(type, reader, writer);
    }

    /** {@inheritDoc} */
    @Override public void processOutStream(int type, BinaryRawWriterEx writer) throws IgniteCheckedException {
        switch (type) {
            case OP_CACHE_CONFIG_PARAMETERS:
                TransactionConfiguration txCfg = platformCtx.kernalContext().config().getTransactionConfiguration();

                writer.writeInt(txCfg.getDefaultTxConcurrency().ordinal());
                writer.writeInt(txCfg.getDefaultTxIsolation().ordinal());
                writer.writeLong(txCfg.getDefaultTxTimeout());

                break;

            case OP_METRICS:
                TransactionMetrics metrics = txs.metrics();

                writer.writeTimestamp(new Timestamp(metrics.commitTime()));
                writer.writeTimestamp(new Timestamp(metrics.rollbackTime()));
                writer.writeInt(metrics.txCommits());
                writer.writeInt(metrics.txRollbacks());

                break;

            default:
                super.processOutStream(type, writer);
        }
    }
}
