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
import org.apache.ignite.internal.GridKernalContextImpl;
import org.apache.ignite.internal.processors.cache.GridCacheMvccManager;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.transactions.TransactionMetrics;
import org.apache.ignite.transactions.TransactionState;

/**
 * Tx metrics adapter.
 */
public class TransactionMetricsAdapter implements TransactionMetrics, Externalizable {
    /** Grid kernal context. */
    private final GridKernalContext gridKernalCtx;

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

    /**
     * Create TransactionMetricsAdapter.
     */
    public TransactionMetricsAdapter() {
        this(null);
    }

    /**
     * @param ctx Kernal context.
     */
    public TransactionMetricsAdapter(GridKernalContext ctx) {
        gridKernalCtx = ctx;
    }

    /** {@inheritDoc} */
    @Override public long commitTime() {
        return commitTime;
    }

    /** {@inheritDoc} */
    @Override public long rollbackTime() {
        return rollbackTime;
    }

    /** {@inheritDoc} */
    @Override public int txCommits() {
        return txCommits;
    }

    /** {@inheritDoc} */
    @Override public int txRollbacks() {
        return txRollbacks;
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
        commitTime = U.currentTimeMillis();

        txCommits++;
    }

    /**
     * Transaction rollback callback.
     */
    public void onTxRollback() {
        rollbackTime = U.currentTimeMillis();

        txRollbacks++;
    }

    /**
     * Reset.
     */
    public void reset() {
        commitTime = 0;
        txCommits = 0;
        rollbackTime = 0;
        txRollbacks = 0;
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

        return mvccManager.lockedKeys().size() + mvccManager.nearLockedKeys().size();
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransactionMetricsAdapter.class, this);
    }
}