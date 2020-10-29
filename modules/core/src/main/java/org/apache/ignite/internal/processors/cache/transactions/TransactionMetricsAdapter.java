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
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.sources.TransactionMetricSource;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.transactions.TransactionMetrics;

/**
 * Tx metrics adapter.
 * @deprecated Should be removed in Apache Ignite 3.0
 */
@Deprecated
public class TransactionMetricsAdapter implements TransactionMetrics {
    /** Metric name for total system time on node. */
    public static final String METRIC_TOTAL_SYSTEM_TIME = "totalNodeSystemTime";

    /** Metric name for system time histogram on node. */
    public static final String METRIC_SYSTEM_TIME_HISTOGRAM = "nodeSystemTimeHistogram";

    /** Metric name for total user time on node. */
    public static final String METRIC_TOTAL_USER_TIME = "totalNodeUserTime";

    /** Metric name for user time histogram on node. */
    public static final String METRIC_USER_TIME_HISTOGRAM = "nodeUserTimeHistogram";

    /** Grid kernal context. */
    private final GridKernalContext ctx;

    /** Transaction metric source. */
    private final TransactionMetricSource src;

    /**
     * @param ctx Kernal context.
     */
    public TransactionMetricsAdapter(GridKernalContext ctx, TransactionMetricSource src) {
        this.ctx = ctx;

        this.src = src;
    }

    /** {@inheritDoc} */
    @Override public long commitTime() {
        if (!src.enabled())
            return -1;

        return src.lastCommitTime();
    }

    /** {@inheritDoc} */
    @Override public long rollbackTime() {
        if (!src.enabled())
            return -1;

        return src.lastRollbackTime();
    }

    /** {@inheritDoc} */
    @Override public int txCommits() {
        if (!src.enabled())
            return -1;

        return src.txCommits();
    }

    /** {@inheritDoc} */
    @Override public int txRollbacks() {
        if (!src.enabled())
            return -1;

        return src.txRollbacks();
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> getAllOwnerTransactions() {
        if (!src.enabled())
            return Collections.emptyMap();

        return src.allTransactionsOwners();
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> getLongRunningOwnerTransactions(int duration) {
        if (!src.enabled())
            return Collections.emptyMap();

        return src.longRunningTransactions(duration);
    }

    /** {@inheritDoc} */
    @Override public long getTransactionsCommittedNumber() {
        return txCommits();
    }

    /** {@inheritDoc} */
    @Override public long getTransactionsRolledBackNumber() {
        return txRollbacks();
    }

    /** {@inheritDoc} */
    @Override public long getTransactionsHoldingLockNumber() {
        if (!src.enabled())
            return -1;

        return src.transactionsHoldingLockNumber();
    }

    /** {@inheritDoc} */
    @Override public long getLockedKeysNumber() {
        if (!src.enabled())
            return -1;

        return src.lockedKeysNumber();
    }

    /** {@inheritDoc} */
    @Override public long getOwnerTransactionsNumber() {
        if (!src.enabled())
            return -1;

        return src.activeLocallyInitiatedTransactionsCount();
    }

    /**
     * Transaction commit callback.
     */
    public void onTxCommit() {
        src.onTxCommit();
    }

    /**
     * Transaction rollback callback.
     */
    public void onTxRollback() {
        src.onTxRollback();
    }

    /**
     * Callback for completion of near transaction. Writes metrics of single near transaction.
     *
     * @param sysTime Transaction system time.
     * @param userTime Transaction user time.
     */
    public void onNearTxComplete(long sysTime, long userTime) {
        src.onNearTxComplete(sysTime, userTime);
    }

    /**
     * Reset.
     */
    public void reset() {
        src.reset();
    }

    /** @return Current metrics values. */
    public TransactionMetrics snapshot() {
        return new TransactionMetricsSnapshot(this);
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
