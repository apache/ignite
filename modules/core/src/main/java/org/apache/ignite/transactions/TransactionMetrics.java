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

package org.apache.ignite.transactions;

import java.util.Map;

/**
 * Transaction metrics, shared across all caches.
 */
public interface TransactionMetrics {
    /**
     * Gets last time transaction was committed.
     *
     * @return Last commit time.
     */
    public long commitTime();

    /**
     * Gets last time transaction was rollback.
     *
     * @return Last rollback time.
     */
    public long rollbackTime();

    /**
     * Gets total number of transaction commits.
     *
     * @return Number of transaction commits.
     */
    public int txCommits();

    /**
     * Gets total number of transaction rollbacks.
     *
     * @return Number of transaction rollbacks.
     */
    public int txRollbacks();

    /**
     * Gets a map of all transactions for which the local node is the originating node.
     *
     * @return Map of local node owning transactions.
     */
    public Map<String, String> getAllOwnerTransactions();

    /**
     * Gets a map of all transactions for which the local node is the originating node and which duration
     * exceeds the given duration.
     *
     * @return Map of local node owning transactions which duration is longer than {@code duration}.
     */
    public Map<String, String> getLongRunningOwnerTransactions(int duration);

    /**
     * The number of transactions which were committed on the local node.
     *
     * @return The number of transactions which were committed on the local node.
     */
    public long getTransactionsCommittedNumber();

    /**
     * The number of transactions which were rolled back on the local node.
     *
     * @return The number of transactions which were rolled back on the local node.
     */
    public long getTransactionsRolledBackNumber();

    /**
     * The number of active transactions on the local node holding at least one key lock.
     *
     * @return The number of active transactions holding at least one key lock.
     */
    public long getTransactionsHoldingLockNumber();

    /**
     * The number of keys locked on the node.
     *
     * @return The number of keys locked on the node.
     */
    public long getLockedKeysNumber();

    /**
     * The number of active transactions for which this node is the initiator. Effectively, this method is
     * semantically equivalent to {@code getAllOwnerTransactions.size()}.
     *
     * @return The number of active transactions for which this node is the initiator.
     */
    public long getOwnerTransactionsNumber();
}
