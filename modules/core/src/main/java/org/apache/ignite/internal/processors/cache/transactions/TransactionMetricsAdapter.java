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
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheMvccManager;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.IntMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.transactions.TransactionMetrics;
import org.apache.ignite.transactions.TransactionState;

import static org.apache.ignite.internal.processors.metric.GridMetricManager.TX_METRICS;

/**
 * Tx metrics adapter.
 */
public class TransactionMetricsAdapter implements TransactionMetrics {
    /** Metric name for total system time on node. */
    public static final String METRIC_TOTAL_SYSTEM_TIME = "totalNodeSystemTime";

    /** Metric name for system time histogram on node. */
    public static final String METRIC_SYSTEM_TIME_HISTOGRAM = "nodeSystemTimeHistogram";

    /** Metric name for total user time on node. */
    public static final String METRIC_TOTAL_USER_TIME = "totalNodeUserTime";

    /** Metric name for user time histogram on node. */
    public static final String METRIC_USER_TIME_HISTOGRAM = "nodeUserTimeHistogram";

    /** Histogram buckets for metrics of system and user time. */
    public static final long[] METRIC_TIME_BUCKETS =
        new long[] { 1, 2, 4, 8, 16, 25, 50, 75, 100, 250, 500, 750, 1000, 3000, 5000, 10000, 25000, 60000};

    /** Grid kernal context. */
    private final GridKernalContext gridKernalCtx;

    /** Number of transaction commits. */
    private final IntMetricImpl txCommits;

    /** Number of transaction rollbacks. */
    private final IntMetricImpl txRollbacks;

    /** Last commit time. */
    private final AtomicLongMetric commitTime;

    /** Last rollback time. */
    private final AtomicLongMetric rollbackTime;

    /** Holds the reference to metric for total system time on node.*/
    private LongAdderMetric totalTxSystemTime;

    /** Holds the reference to metric for total user time on node. */
    private LongAdderMetric totalTxUserTime;

    /** Holds the reference to metric for system time histogram on node. */
    private HistogramMetricImpl txSystemTimeHistogram;

    /** Holds the reference to metric for user time histogram on node. */
    private HistogramMetricImpl txUserTimeHistogram;

    /**
     * @param ctx Kernal context.
     */
    public TransactionMetricsAdapter(GridKernalContext ctx) {
        gridKernalCtx = ctx;

        MetricRegistry mreg = gridKernalCtx.metric().registry(TX_METRICS);

        txCommits = mreg.intMetric("txCommits", "Number of transaction commits.");
        txRollbacks = mreg.intMetric("txRollbacks", "Number of transaction rollbacks.");
        commitTime = mreg.longMetric("commitTime", "Last commit time.");
        rollbackTime = mreg.longMetric("rollbackTime", "Last rollback time.");
        totalTxSystemTime = mreg.longAdderMetric(METRIC_TOTAL_SYSTEM_TIME, "Total transactions system time on node.");
        totalTxUserTime = mreg.longAdderMetric(METRIC_TOTAL_USER_TIME, "Total transactions user time on node.");

        txSystemTimeHistogram = mreg.histogram(
            METRIC_SYSTEM_TIME_HISTOGRAM,
            METRIC_TIME_BUCKETS,
            "Transactions system times on node represented as histogram."
        );

        txUserTimeHistogram = mreg.histogram(
            METRIC_USER_TIME_HISTOGRAM,
            METRIC_TIME_BUCKETS,
            "Transactions user times on node represented as histogram."
        );
    }

    /** Callback invoked when {@link IgniteTxManager} started. */
    public void onTxManagerStarted() {
        MetricRegistry mreg = gridKernalCtx.metric().registry(TX_METRICS);

        mreg.register("AllOwnerTransactions",
            this::getAllOwnerTransactions,
            Map.class,
            "Map of local node owning transactions.");

        mreg.register("TransactionsHoldingLockNumber",
            this::getTransactionsHoldingLockNumber,
            "The number of active transactions holding at least one key lock.");

        mreg.register("LockedKeysNumber",
            this::txLockedKeysNum,
            "The number of keys locked on the node.");

        mreg.register("OwnerTransactionsNumber",
            this::nearTxNum,
            "The number of active transactions for which this node is the initiator.");
    }

    /** {@inheritDoc} */
    @Override public long commitTime() {
        return commitTime.value();
    }

    /** {@inheritDoc} */
    @Override public long rollbackTime() {
        return rollbackTime.value();
    }

    /** {@inheritDoc} */
    @Override public int txCommits() {
        return txCommits.value();
    }

    /** {@inheritDoc} */
    @Override public int txRollbacks() {
        return txRollbacks.value();
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> getAllOwnerTransactions() {
        return getNearTxs(0);
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> getLongRunningOwnerTransactions(final int duration) {
        return getNearTxs(duration);
    }

    /** {@inheritDoc} */
    @Override public long getTransactionsCommittedNumber() {
        return gridKernalCtx.cache().context().txMetrics().txCommits();
    }

    /** {@inheritDoc} */
    @Override public long getTransactionsRolledBackNumber() {
        return gridKernalCtx.cache().context().txMetrics().txRollbacks();
    }

    /** {@inheritDoc} */
    @Override public long getTransactionsHoldingLockNumber() {
        return txHoldingLockNum();
    }

    /** {@inheritDoc} */
    @Override public long getLockedKeysNumber() {
        return txLockedKeysNum();
    }

    /** {@inheritDoc} */
    @Override public long getOwnerTransactionsNumber() {
        return nearTxNum();
    }

    /**
     * Transaction commit callback.
     */
    public void onTxCommit() {
        commitTime.value(U.currentTimeMillis());

        txCommits.increment();
    }

    /**
     * Transaction rollback callback.
     */
    public void onTxRollback() {
        rollbackTime.value(U.currentTimeMillis());

        txRollbacks.increment();
    }

    /**
     * Callback for completion of near transaction. Writes metrics of single near transaction.
     *
     * @param systemTime Transaction system time.
     * @param userTime Transaction user time.
     */
    public void onNearTxComplete(long systemTime, long userTime) {
        if (systemTime >= 0) {
            totalTxSystemTime.add(systemTime);

            txSystemTimeHistogram.value(systemTime);
        }

        if (userTime >= 0) {
            totalTxUserTime.add(userTime);

            txUserTimeHistogram.value(userTime);
        }
    }

    /**
     * Reset.
     */
    public void reset() {
        commitTime.reset();
        txCommits.reset();
        rollbackTime.reset();
        txRollbacks.reset();
    }

    /** @return Current metrics values. */
    public TransactionMetrics snapshot() {
        return new TransactionMetricsSnapshot(this);
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

    /**
     * @param id Id.
     */
    private String composeNodeInfo(final UUID id) {
        final ClusterNode node = gridKernalCtx.discovery().node(id);
        if (node == null)
            return "";

        return String.format("%s %s",
            node.id(),
            node.hostNames());
    }

    /**
     * @param ids Ids.
     */
    private String composeNodeInfo(final Set<UUID> ids) {
        final GridStringBuilder sb = new GridStringBuilder();

        sb.a("[");

        String delim = "";

        for (UUID id : ids) {
            sb
                .a(delim)
                .a(composeNodeInfo(id));
            delim = ", ";
        }

        sb.a("]");

        return sb.toString();
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

        final Long duration = System.currentTimeMillis() - tx.startTime();

        return top + "DURATION: " + duration;
    }

    /**
     *
     */
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

        return F.viewReadOnly(gridKernalCtx.cache().context().tm().activeTransactions(), c, pred);
    }

    /**
     *
     */
    private long nearTxNum() {
        IgnitePredicate<IgniteInternalTx> pred = new IgnitePredicate<IgniteInternalTx>() {
            @Override public boolean apply(IgniteInternalTx tx) {
                return tx.local() && tx.near();
            }
        };

        return F.size(gridKernalCtx.cache().context().tm().activeTransactions(), pred);
    }

    /**
     * Count total number of holding locks on local node.
     */
    private long txHoldingLockNum() {
        long holdingLockCounter = 0;

        IgniteTxManager tm = gridKernalCtx.cache().context().tm();

        for (IgniteInternalTx tx : tm.activeTransactions()) {
            if ((tx.optimistic() && tx.state() == TransactionState.ACTIVE) || tx.empty() || !tx.local())
                continue;

            holdingLockCounter++;
        }

        return holdingLockCounter;
    }

    /**
     * Count total number of locked keys on local node.
     */
    private long txLockedKeysNum() {
        GridCacheMvccManager mvccManager = gridKernalCtx.cache().context().mvcc();

        if (mvccManager == null)
            return 0;

        return mvccManager.lockedKeys().size() + mvccManager.nearLockedKeys().size();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransactionMetricsAdapter.class, this);
    }

    /** Transaction metrics snapshot. */
    public static class TransactionMetricsSnapshot implements TransactionMetrics, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Number of transaction commits. */
        private volatile int txCommits;

        /** Number of transaction rollbacks. */
        private volatile int txRollbacks;

        /** Last commit time. */
        private volatile long commitTime;

        /** Last rollback time. */
        private volatile long rollbackTime;

        /** Transaction metrics adapter. */
        private volatile TransactionMetricsAdapter adapter;

        /** Required by {@link Externalizable}. */
        public TransactionMetricsSnapshot() {
            this(null);
        }

        /**
         * @param adapter Transaction metrics adapter.
         */
        public TransactionMetricsSnapshot(TransactionMetricsAdapter adapter) {
            this.adapter = adapter;
        }

        /** {@inheritDoc} */
        @Override public long commitTime() {
            return adapter != null ? adapter.commitTime() : commitTime;
        }

        /** {@inheritDoc} */
        @Override public long rollbackTime() {
            return adapter != null ? adapter.rollbackTime() : rollbackTime;
        }

        /** {@inheritDoc} */
        @Override public int txCommits() {
            return adapter != null ? adapter.txCommits() : txCommits;
        }

        /** {@inheritDoc} */
        @Override public int txRollbacks() {
            return adapter != null ? adapter.txRollbacks() : txRollbacks;
        }

        /** {@inheritDoc} */
        @Override public Map<String, String> getAllOwnerTransactions() {
            return adapter != null ? adapter.getAllOwnerTransactions() : null;
        }

        /** {@inheritDoc} */
        @Override public Map<String, String> getLongRunningOwnerTransactions(int duration) {
            return adapter != null ? adapter.getLongRunningOwnerTransactions(duration) : null;
        }

        /** {@inheritDoc} */
        @Override public long getTransactionsCommittedNumber() {
            return adapter != null ? adapter.getTransactionsCommittedNumber() : 0;
        }

        /** {@inheritDoc} */
        @Override public long getTransactionsRolledBackNumber() {
            return adapter != null ? adapter.getTransactionsRolledBackNumber() : 0;
        }

        /** {@inheritDoc} */
        @Override public long getTransactionsHoldingLockNumber() {
            return adapter != null ? adapter.getTransactionsHoldingLockNumber() : 0;
        }

        /** {@inheritDoc} */
        @Override public long getLockedKeysNumber() {
            return adapter != null ? adapter.getLockedKeysNumber() : 0;
        }

        /** {@inheritDoc} */
        @Override public long getOwnerTransactionsNumber() {
            return adapter != null ? adapter.getOwnerTransactionsNumber() : 0;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(commitTime);
            out.writeLong(rollbackTime);
            out.writeInt(txCommits);
            out.writeInt(txRollbacks);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            commitTime = in.readLong();
            rollbackTime = in.readLong();
            txCommits = in.readInt();
            txRollbacks = in.readInt();
        }
    }
}
