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

import java.io.Serializable;
import java.util.Map;

/**
 * Transaction metrics, shared across all caches.
 */
public interface TransactionMetrics extends Serializable {
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
     * All near transactions
     *
     * @return near transactions.
     */
    public Map<String, String> getAllNearTxs();

    /**
     * Long running near transactions
     *
     * @return near transactions.
     */
    public Map<String, String> getLongRunningNearTxs(int duration);

    /**
     * The number of transactions which were committed.
     *
     * @return number of transactions which were committed.
     */
    public long getTxCommittedNum();

    /**
     * The number of transactions which were rollback.
     *
     * @return number of transactions which were rollback.
     */
    public long getTxRolledBackNum();

    /**
     * The number of active transactions holding at least one key lock.
     *
     * @return number of active transactions holding at least one key lock.
     */
    public long getTxHoldingLockNum();

    /**
     * The number of keys locked on the node.
     *
     * @return number of keys locked on the node.
     */
    public long getLockedKeysNum();

    /**
     * The number of active transactions for which this node is the initiator.
     *
     * @return number of active transactions for which this node is the initiator.
     */
    public long getOwnerTxNum();
}