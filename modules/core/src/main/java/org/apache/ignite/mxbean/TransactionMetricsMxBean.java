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

package org.apache.ignite.mxbean;

import java.util.Map;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.transactions.TransactionMetrics;

/**
 * Transactions MXBean interface.
 *
 * @deprecated Use {@link GridMetricManager} instead.
 */
@Deprecated
@MXBeanDescription("MBean that provides access to Ignite transactions.")
public interface TransactionMetricsMxBean extends TransactionMetrics {
    /**
     * All near transactions
     *
     * @return near transactions.
     */
    @MXBeanDescription("All near transactions.")
    @Override public Map<String, String> getAllOwnerTransactions();

    /**
     * Long running near transactions
     *
     * @return near transactions.
     */
    @MXBeanDescription("Long running near transactions.")
    @Override public Map<String, String> getLongRunningOwnerTransactions(
        @MXBeanParameter(name = "duration", description = "Duration, at least (ms).") int duration
    );

    /**
     * The number of transactions which were committed.
     *
     * @return number of transactions which were committed.
     */
    @MXBeanDescription("The number of transactions which were committed.")
    @Override public long getTransactionsCommittedNumber();

    /**
     * The number of transactions which were rollback.
     *
     * @return number of transactions which were rollback.
     */
    @MXBeanDescription("The number of transactions which were rollback.")
    @Override public long getTransactionsRolledBackNumber();

    /**
     * The number of active transactions holding at least one key lock.
     *
     * @return number of active transactions holding at least one key lock.
     */
    @MXBeanDescription("The number of active transactions holding at least one key lock.")
    @Override public long getTransactionsHoldingLockNumber();

    /**
     *  The number of keys locked on the node.
     *
     * @return number of keys locked on the node.
     */
    @MXBeanDescription("The number of keys locked on the node.")
    @Override public long getLockedKeysNumber();

    /**
     * The number of active transactions for which this node is the initiator.
     *
     * @return number of active transactions for which this node is the initiator.
     */
    @MXBeanDescription("The number of active transactions for which this node is the initiator.")
    @Override public long getOwnerTransactionsNumber();

    /**
     * The last time, when transaction was commited.
     *
     * @return last time, when transaction was commited.
     */
    @MXBeanDescription("Last commit time.")
    @Override long commitTime();

    /**
     * The last time, when transaction was rollbacked.
     *
     * @return last time, when transaction was rollbacked.
     */
    @MXBeanDescription("Last rollback time.")
    @Override long rollbackTime();

    /**
     * The total number of commited transactions.
     *
     * @return total number of commited transactions.
     */
    @MXBeanDescription("Number of transaction commits.")
    @Override int txCommits();

    /**
     * Tne total number of rollbacked transactions.
     *
     * @return total number of rollbacked transactions.
     */
    @MXBeanDescription("Number of transaction rollbacks.")
    @Override int txRollbacks();
}
