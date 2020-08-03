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

package org.apache.ignite.spi.systemview.view;

import java.util.Collection;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxState;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;

/**
 * Transaction representation for a {@link SystemView}.
 */
public class TransactionView {
    /** Transaction. */
    private final IgniteInternalTx tx;

    /**
     * @param tx Transaction.
     */
    public TransactionView(IgniteInternalTx tx) {
        this.tx = tx;
    }

    /**
     * @return Local node ID.
     * @see IgniteInternalTx#nodeId()
     */
    public UUID localNodeId() {
        return tx.nodeId();
    }

    /**
     * @return ID of the thread in which this transaction started.
     * @see IgniteInternalTx#threadId()
     */
    public long threadId() {
        return tx.threadId();
    }

    /**
     * @return Start time of this transaction on this node.
     * @see IgniteInternalTx#startTime()
     */
    @Order(4)
    public long startTime() {
        return tx.startTime();
    }

    /**
     * @return Isolation level.
     * @see IgniteInternalTx#isolation()
     */
    @Order(5)
    public TransactionIsolation isolation() {
        return tx.isolation();
    }

    /**
     * @return Concurrency mode.
     * @see IgniteInternalTx#concurrency()
     */
    @Order(6)
    public TransactionConcurrency concurrency() {
        return tx.concurrency();
    }

    /**
     * @return Current transaction state.
     * @see IgniteInternalTx#state()
     */
    @Order(1)
    public TransactionState state() {
        return tx.state();
    }

    /**
     * @return Transaction timeout value.
     * @see IgniteInternalTx#timeout()
     */
    public long timeout() {
        return tx.timeout();
    }

    /**
     * @return {@code True} if transaction was started implicitly.
     * @see IgniteInternalTx#implicit()
     */
    public boolean implicit() {
        return tx.implicit();
    }

    /**
     * @return Transaction UID.
     * @see IgniteInternalTx#xid()
     */
    @Order(2)
    public IgniteUuid xid() {
        return tx.xid();
    }

    /**
     * @return {@code True} if transaction is started for system cache.
     * @see IgniteInternalTx#system()
     */
    public boolean system() {
        return tx.system();
    }

    /**
     * @return Flag indicating whether transaction is implicit with only one key.
     * @see IgniteInternalTx#implicitSingle()
     */
    public boolean implicitSingle() {
        return tx.implicitSingle();
    }

    /**
     * @return {@code True} if near transaction.
     * @see IgniteInternalTx#near()
     */
    public boolean near() {
        return tx.near();
    }

    /**
     * @return {@code True} if DHT transaction.
     * @see IgniteInternalTx#dht()
     */
    public boolean dht() {
        return tx.dht();
    }

    /**
     * @return {@code True} if dht colocated transaction.
     * @see IgniteInternalTx#colocated()
     */
    public boolean colocated() {
        return tx.colocated();
    }

    /**
     * @return {@code True} if transaction is local, {@code false} if it's remote.
     * @see IgniteInternalTx#local()
     */
    public boolean local() {
        return tx.local();
    }

    /**
     * @return Subject ID initiated this transaction.
     * @see IgniteInternalTx#subjectId()
     */
    public UUID subjectId() {
        return tx.subjectId();
    }

    /**
     * @return Label of transaction or {@code null} if there was not set.
     * @see IgniteInternalTx#label()
     */
    @Order(3)
    public String label() {
        return tx.label();
    }

    /**
     * @return {@code True} if transaction is a one-phase-commit transaction.
     * @see IgniteInternalTx#onePhaseCommit()
     */
    public boolean onePhaseCommit() {
        return tx.onePhaseCommit();
    }

    /**
     * @return {@code True} if transaction has at least one internal entry.
     */
    public boolean internal() {
        return tx.internal();
    }

    /**
     * @return Originating node id.
     * @see IgniteInternalTx#originatingNodeId()
     */
    @Order
    public UUID originatingNodeId() {
        return tx.originatingNodeId();
    }

    /**
     * @return Other node id.
     * @see IgniteInternalTx#otherNodeId()
     */
    public UUID otherNodeId() {
        return tx.otherNodeId();
    }

    /**
     * @return Topology version.
     * @see IgniteInternalTx#topologyVersion()
     */
    public String topVer() {
        return Objects.toString(tx.topologyVersion());
    }

    /**
     * @return Duration in millis.
     * @see IgniteInternalTx#startTime()
     */
    public long duration() {
        return U.currentTimeMillis() - tx.startTime();
    }

    /**
     * @return Count of the cache keys participatint in transaction.
     * @see IgniteInternalTx#allEntries()
     */
    @Order(7)
    public int keysCount() {
        Collection<IgniteTxEntry> entries = tx.allEntries();

        if (entries == null)
            return 0;

        return entries.size();
    }

    /**
     * @return Id of the cashes participating in transaction.
     * @see IgniteTxState#cacheIds()
     */
    @Order(8)
    public String cacheIds() {
        GridIntList cacheIds = tx.txState().cacheIds();

        if (cacheIds == null)
            return null;

        int sz = cacheIds.size();

        if (sz == 0)
            return null;

        //GridIntList is not synchronized. If we fail while iterating just ignore.
        try {
            SB b = new SB();

            for (int i = 0; i < sz; i++) {
                if (i != 0)
                    b.a(',');

                b.a(cacheIds.get(i));
            }

            return b.toString();
        }
        catch (Throwable e) {
            return null;
        }

    }
}
