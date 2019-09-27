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

import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
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

    /** @return Local node ID. */
    public UUID localNodeId() {
        return tx.nodeId();
    }

    /** @return ID of the thread in which this transaction started. */
    public long threadId() {
        return tx.threadId();
    }

    /** @return Start time of this transaction on this node. */
    @Order(4)
    public long startTime() {
        return tx.startTime();
    }

    /** @return Isolation level. */
    @Order(5)
    public TransactionIsolation isolation() {
        return tx.isolation();
    }

    /** @return Concurrency mode. */
    @Order(6)
    public TransactionConcurrency concurrency() {
        return tx.concurrency();
    }

    /** @return Current transaction state. */
    @Order(1)
    public TransactionState state() {
        return tx.state();
    }

    /** @return Transaction timeout value. */
    public long timeout() {
        return tx.timeout();
    }

    /** @return {@code True} if transaction was started implicitly. */
    public boolean implicit() {
        return tx.implicit();
    }

    /** @return Transaction UID. */
    @Order(2)
    public IgniteUuid xid() {
        return tx.xid();
    }

    /** @return {@code True} if transaction is started for system cache. */
    public boolean system() {
        return tx.system();
    }

    /** @return Flag indicating whether transaction is implicit with only one key. */
    public boolean implicitSingle() {
        return tx.implicitSingle();
    }

    /** @return {@code True} if near transaction. */
    public boolean near() {
        return tx.near();
    }

    /** @return {@code True} if DHT transaction. */
    public boolean dht() {
        return tx.dht();
    }

    /** @return {@code True} if dht colocated transaction. */
    public boolean colocated() {
        return tx.colocated();
    }

    /** @return {@code True} if transaction is local, {@code false} if it's remote. */
    public boolean local() {
        return tx.local();
    }

    /** @return Subject ID initiated this transaction. */
    public UUID subjectId() {
        return tx.subjectId();
    }

    /** @return Label of transaction or {@code null} if there was not set. */
    @Order(3)
    public String label() {
        return tx.label();
    }

    /** @return {@code True} if transaction is a one-phase-commit transaction. */
    public boolean onePhaseCommit() {
        return tx.onePhaseCommit();
    }

    /** @return {@code True} if transaction has at least one internal entry. */
    public boolean internal() {
        return tx.internal();
    }

    /** @return Originating node id. */
    @Order
    public UUID originatingNodeId() {
        return tx.originatingNodeId();
    }

    /** @return Other node id. */
    public UUID otherNodeId() {
        return tx.otherNodeId();
    }

    /** @return Topology version. */
    public String topVer() {
        return Objects.toString(tx.topologyVersion());
    }

    /** @return Duration in millis. */
    public long duration() {
        return U.currentTimeMillis() - tx.startTime();
    }

    /** @return Keys count. */
    @Order(7)
    public int keysCount() {
        return tx.allEntries().size();
    }
}
