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

package org.apache.ignite.internal;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.mxbean.TxMXBean;
import org.apache.ignite.transactions.TransactionState;

/**
 * Transactions MXBean implementation.
 */
public class TxMXBeanImpl implements TxMXBean {
    /** Grid kernal context. */
    private final GridKernalContextImpl gridKernalCtx;

    /**
     * @param ctx Context.
     */
    TxMXBeanImpl(GridKernalContextImpl ctx) {
        this.gridKernalCtx = ctx;
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> getAllNearTxs() {
        return getNearTxs(0);
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> getLongRunningNearTxs(final int duration) {
        return getNearTxs(duration);
    }

    /** {@inheritDoc} */
    @Override public String stopTransaction(String txId) throws IgniteCheckedException {
        final Collection<GridNearTxLocal> txs = nearTxs(0);
        if (!F.isEmpty(txId))
            for (GridNearTxLocal tx : txs)
                if (tx.xid().toString().equals(txId)) {
                    tx.close();
                    return String.format("Transaction %s is %s", tx.xid(), tx.state());
                }
        throw new IgniteCheckedException("Transaction with id " + txId + " is not found");
    }

    /** {@inheritDoc} */
    @Override public long getTxCommittedNum() {
        return gridKernalCtx.cache().context().txMetrics().txCommits();
    }

    /** {@inheritDoc} */
    @Override public long getTxRolledBackNum() {
        return gridKernalCtx.cache().context().txMetrics().txRollbacks();
    }

    /** {@inheritDoc} */
    @Override public long getTxHoldingLockNum() {
        return 0; // TODO
    }

    /** {@inheritDoc} */
    @Override public long getLockedKeysNum() {
        return 0; // TODO
    }

    /** {@inheritDoc} */
    @Override public long getOwnerTxNum() {
        return nearTxNum();
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TxMXBeanImpl.class, this);
    }

}


