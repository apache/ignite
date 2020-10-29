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

package org.apache.ignite.internal.processors.metric.sources;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheMvccManager;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.metric.MetricRegistryBuilder;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.IntMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.transactions.TransactionState;

/**
 * Metric source for transactional metrics.
 */
public class TransactionMetricSource extends AbstractMetricSource<TransactionMetricSource.Holder> {
    /** Transaction metrics prefix. */
    public static final String TX_METRICS = "tx";

    /** Histogram buckets for metrics of system and user time. */
    private static final long[] METRIC_TIME_BUCKETS =
            new long[] { 1, 2, 4, 8, 16, 25, 50, 75, 100, 250, 500, 750, 1000, 3000, 5000, 10000, 25000, 60000};

    /**
     * Creates transaction metric source.
     *
     * @param ctx Kernal context.
     */
    public TransactionMetricSource(GridKernalContext ctx) {
        super(TX_METRICS, ctx);
    }

    /** {@inheritDoc} */
    @Override protected void init(MetricRegistryBuilder bldr, Holder hldr) {
        hldr.txCommits = bldr.intMetric("txCommits", "Number of transaction commits.");

        hldr.txRollbacks = bldr.intMetric("txRollbacks", "Number of transaction rollbacks.");

        hldr.lastCommitTime = bldr.longMetric("lastCommitTime", "Last commit time.");

        hldr.lastRollbackTime = bldr.longMetric("lastRollbackTime", "Last rollback time.");

        hldr.totalTxSysTime = bldr.longAdderMetric("totalNodeSystemTime", "Total transactions system time on node.");

        hldr.totalTxUserTime = bldr.longAdderMetric("totalNodeUserTime", "Total transactions user time on node.");

        hldr.txSysTimeHistogram = bldr.histogram(
                "nodeSystemTimeHistogram",
                METRIC_TIME_BUCKETS,
                "Transactions system times on node represented as histogram."
        );

        hldr.txUserTimeHistogram = bldr.histogram(
                "nodeUserTimeHistogram",
                METRIC_TIME_BUCKETS,
                "Transactions user times on node represented as histogram."
        );

        bldr.register("AllOwnerTransactions",
                this::allTransactionsOwners,
                Map.class,
                "Map of local node owning transactions.");

        bldr.register("TransactionsHoldingLockNumber",
                this::transactionsHoldingLockNumber,
                "The number of active transactions holding at least one key lock.");

        bldr.register("LockedKeysNumber",
                this::lockedKeysNumber,
                "The number of keys locked on the node.");

        bldr.register("OwnerTransactionsNumber",
                this::nearTxNum,
                "The number of active transactions for which this node is the initiator.");
    }

    /**
     * Returns last time transaction was committed.
     *
     * @return Last commit time.
     */
    public long lastCommitTime() {
        Holder hldr = holder();

        return hldr != null ? hldr.lastCommitTime.value() : -1;
    }

    /**
     * Returns last time transaction was rollback.
     *
     * @return Last rollback time.
     */
    public long lastRollbackTime() {
        Holder hldr = holder();

        return hldr != null ? hldr.lastRollbackTime.value() : -1;
    }

    /**
     * Returns total number of transaction commits.
     *
     * @return Number of transaction commits.
     */
    public int txCommits() {
        Holder hldr = holder();

        return hldr != null ? hldr.txCommits.value() : -1;
    }

    /**
     * Returns total number of transaction rollbacks.
     *
     * @return Number of transaction rollbacks.
     */
    public int txRollbacks() {
        Holder hldr = holder();

        return hldr != null ? hldr.txRollbacks.value() : -1;
    }

    /**
     * Returns a map of all transactions for which the local node is the originating node.
     *
     * @return Map of local node owning transactions.
     * @deprecated Should be removed in Apache Ignite 3.0. It isn't metric. Consider another way for
     * providing this info. E.g. system views.
     */
    @Deprecated
    public Map<String, String> allTransactionsOwners() {
        return getNearTxs(0);
    }

    /**
     * Returns a map of all transactions for which the local node is the originating node and which duration
     * exceeds the given duration.
     *
     * @return Map of local node owning transactions which duration is longer than {@code duration}.
     * @deprecated Should be removed in Apache Ignite 3.0. It isn't metric. Consider another way for
     * providing this info. E.g. system views.
     */
    @Deprecated
    public Map<String, String> longRunningTransactions(int duration) {
        return getNearTxs(duration);
    }

    /**
     * The number of active transactions on the local node holding at least one key lock.
     *
     * @return The number of active transactions holding at least one key lock.
     */
    //TODO: Check logic. It seems that we just count tx numbers regardless of locked keys.
    public long transactionsHoldingLockNumber() {
        long holdingLockCntr = 0;

        IgniteTxManager tm = ctx().cache().context().tm();

        for (IgniteInternalTx tx : tm.activeTransactions()) {
            if ((tx.optimistic() && tx.state() == TransactionState.ACTIVE) || tx.empty() || !tx.local())
                continue;

            holdingLockCntr++;
        }

        return holdingLockCntr;
    }

    /**
     * The number of active transactions for which this node is the initiator.
     *
     * @return The number of active transactions for which this node is the initiator.
     */
    public long activeLocallyInitiatedTransactionsCount() {
        return nearTxNum();
    }

    /**
     * Transaction commit callback.
     */
    public void onTxCommit() {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.lastCommitTime.value(U.currentTimeMillis());

            hldr.txCommits.increment();
        }
    }

    /**
     * Transaction rollback callback.
     */
    public void onTxRollback() {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.lastRollbackTime.value(U.currentTimeMillis());

            hldr.txRollbacks.increment();
        }
    }

    /**
     * Callback for completion of near transaction. Writes metrics of single near transaction.
     *
     * @param sysTime Transaction system time.
     * @param userTime Transaction user time.
     */
    public void onNearTxComplete(long sysTime, long userTime) {
        Holder hldr = holder();

        if (hldr != null) {
            if (sysTime >= 0) {
                hldr.totalTxSysTime.add(sysTime);

                hldr.txSysTimeHistogram.value(sysTime);
            }

            if (userTime >= 0) {
                hldr.totalTxUserTime.add(userTime);

                hldr.txUserTimeHistogram.value(userTime);
            }
        }
    }

    /**
     * The number of keys locked on the node.
     *
     * @return The number of keys locked on the node.
     */
    public long lockedKeysNumber() {
        GridCacheMvccManager mvccMgr = ctx().cache().context().mvcc();

        // Could be null on client node.
        if (mvccMgr == null)
            return 0;

        return mvccMgr.lockedKeys().size() + mvccMgr.nearLockedKeys().size();
    }

    /**
     * Reset.
     */
    public void reset() {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.lastCommitTime.reset();

            hldr.txCommits.reset();

            hldr.lastRollbackTime.reset();

            hldr.txRollbacks.reset();
        }
    }

    /**
     * @param duration Duration.
     */
    private Map<String, String> getNearTxs(long duration) {
        final Collection<GridNearTxLocal> txs = nearTxs(duration);

        final HashMap<String, String> res = new HashMap<>(txs.size());

        for (GridNearTxLocal tx : txs)
            res.put(tx.xid().toString(), composeTx(tx));

        return res;
    }

    /** */
    private Collection<GridNearTxLocal> nearTxs(long duration) {
        final long start = System.currentTimeMillis();

        IgniteClosure<IgniteInternalTx, GridNearTxLocal> c = new IgniteClosure<IgniteInternalTx, GridNearTxLocal>() {
            @Override public GridNearTxLocal apply(IgniteInternalTx tx) {
                return ((GridNearTxLocal)tx);
            }
        };

        IgnitePredicate<IgniteInternalTx> pred = new IgnitePredicate<IgniteInternalTx>() {
            @Override public boolean apply(IgniteInternalTx tx) {
                return tx.local() && tx.near() && start - tx.startTime() >= duration;
            }
        };

        return F.viewReadOnly(ctx().cache().context().tm().activeTransactions(), c, pred);
    }

    /**
     * @param tx Transaction.
     */
    private String composeTx(final GridNearTxLocal tx) {
        final TransactionState txState = tx.state();

        String top = txState + ", NEAR, ";

        if (txState == TransactionState.PREPARING) {
            final Map<UUID, Collection<UUID>> transactionNodes = tx.transactionNodes();

            if (!F.isEmpty(transactionNodes)) {
                final Set<UUID> primaryNodes = transactionNodes.keySet();

                if (!F.isEmpty(primaryNodes))
                    top += "PRIMARY: " + composeNodeInfo(primaryNodes) + ", ";
            }
        }

        long duration = System.currentTimeMillis() - tx.startTime();

        return top + "DURATION: " + duration;
    }

    /**
     * @param ids Ids.
     */
    private String composeNodeInfo(final Set<UUID> ids) {
        final GridStringBuilder sb = new GridStringBuilder();

        sb.a('[');

        String delim = "";

        for (UUID id : ids) {
            sb.a(delim).a(composeNodeInfo(id));

            delim = ", ";
        }

        sb.a(']');

        return sb.toString();
    }

    /**
     * @param id Id.
     */
    private String composeNodeInfo(final UUID id) {
        final ClusterNode node = ctx().discovery().node(id);

        if (node == null)
            return "";

        return String.format("%s %s",
                node.id(),
                node.hostNames());
    }

    /** */
    private long nearTxNum() {
        IgnitePredicate<IgniteInternalTx> pred = new IgnitePredicate<IgniteInternalTx>() {
            @Override public boolean apply(IgniteInternalTx tx) {
                return tx.local() && tx.near();
            }
        };

        return F.size(ctx().cache().context().tm().activeTransactions(), pred);
    }

    /** {@inheritDoc} */
    @Override protected Holder createHolder() {
        return new Holder();
    }

    /** */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        /** Number of transaction commits. */
        private IntMetricImpl txCommits;

        /** Number of transaction rollbacks. */
        private IntMetricImpl txRollbacks;

        /** Last commit time. */
        private AtomicLongMetric lastCommitTime;

        /** Last rollback time. */
        private AtomicLongMetric lastRollbackTime;

        /** Holds the reference to metric for total system time on node.*/
        private LongAdderMetric totalTxSysTime;

        /** Holds the reference to metric for total user time on node. */
        private LongAdderMetric totalTxUserTime;

        /** Holds the reference to metric for system time histogram on node. */
        private HistogramMetricImpl txSysTimeHistogram;

        /** Holds the reference to metric for user time histogram on node. */
        private HistogramMetricImpl txUserTimeHistogram;
    }
}
