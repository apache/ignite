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

import java.util.UUID;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;

/**
 * Transaction representation for a {@link SystemView}.
 */
public interface TransactionView {
    /**
     * ID of the node on which this transaction started.
     *
     * @return Originating node ID.
     */
    @Order
    public UUID nodeId();

    /**
     * ID of the thread in which this transaction started.
     *
     * @return Thread ID.
     */
    public long threadId();

    /**
     * Start time of this transaction.
     *
     * @return Start time of this transaction on this node.
     */
    @Order(4)
    public long startTime();

    /**
     * Cache transaction isolation level.
     *
     * @return Isolation level.
     */
    @Order(5)
    public TransactionIsolation isolation();

    /**
     * Cache transaction concurrency mode.
     *
     * @return Concurrency mode.
     */
    @Order(6)
    public TransactionConcurrency concurrency();

    /**
     * Gets current transaction state value.
     *
     * @return Current transaction state.
     */
    @Order(1)
    public TransactionState state();

    /**
     * Gets timeout value in milliseconds for this transaction. If transaction times
     * out prior to it's completion, {@link org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException} will be thrown.
     *
     * @return Transaction timeout value.
     */
    public long timeout();

    /**
     * Flag indicating whether transaction was started automatically by the
     * system or not. System will start transactions implicitly whenever
     * any cache {@code put(..)} or {@code remove(..)} operation is invoked
     * outside of transaction.
     *
     * @return {@code True} if transaction was started implicitly.
     */
    public boolean implicit();

    /**
     * Gets unique identifier for this transaction.
     *
     * @return Transaction UID.
     */
    @Order(2)
    public IgniteUuid xid();

    /**
     * Checks if this is system cache transaction. System transactions are isolated from user transactions
     * because some of the public API methods may be invoked inside user transactions and internally start
     * system cache transactions.
     *
     * @return {@code True} if transaction is started for system cache.
     */
    public boolean system();

    /**
     * @return Flag indicating whether transaction is implicit with only one key.
     */
    public boolean implicitSingle();

    /**
     * @return {@code True} if near transaction.
     */
    public boolean near();

    /**
     * @return {@code True} if DHT transaction.
     */
    public boolean dht();

    /**
     * @return {@code True} if dht colocated transaction.
     */
    public boolean colocated();

    /**
     * @return {@code True} if transaction is local, {@code false} if it's remote.
     */
    public boolean local();

    /**
     * @return Subject ID initiated this transaction.
     */
    public UUID subjectId();

    /**
     * Returns label of transactions.
     *
     * @return Label of transaction or {@code null} if there was not set.
     */
    @Order(3)
    public String label();

    /**
     * @return {@code True} if transaction is a one-phase-commit transaction.
     */
    public boolean onePhaseCommit();

    /**
     * @return {@code True} if transaction has at least one internal entry.
     */
    public boolean internal();
}
