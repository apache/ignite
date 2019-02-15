/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.mxbean;

import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.transactions.TransactionMetrics;

/**
 * Transactions MXBean interface.
 */
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
    @MXBeanParametersNames("duration")
    @MXBeanParametersDescriptions("Duration, at least (ms).")
    @Override public Map<String, String> getLongRunningOwnerTransactions(int duration);

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
}
