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
import org.apache.ignite.IgniteCheckedException;

/**
 * Transactions MXBean interface.
 */
@MXBeanDescription("MBean that provides access to Ignite transactions.")
public interface TxMXBean {
    /**
     * All near transactions
     *
     * @return near transactions.
     */
    @MXBeanDescription("All near transactions.")
    public Map<String, String> getAllNearTxs();

    /**
     * Long running near transactions
     *
     * @return near transactions.
     */
    @MXBeanDescription("Long running near transactions.")
    @MXBeanParametersNames("duration")
    @MXBeanParametersDescriptions("Duration, at least (ms).")
    public Map<String, String> getLongRunningNearTxs(int duration);

    /**
     * Stop transaction.
     *
     * @return Updated status of Transaction.
     * */
    @MXBeanDescription("Stop transaction.")
    @MXBeanParametersNames("txId")
    @MXBeanParametersDescriptions("Transaction id to stop.")
    public String stopTransaction(String txId) throws IgniteCheckedException;


    /**
     * The number of transactions which were committed.
     *
     * @return number of transactions which were committed.
     */
    @MXBeanDescription("The number of transactions which were committed.")
    public long getTxCommittedNum();

    /**
     * The number of transactions which were rollback.
     *
     * @return number of transactions which were rollback.
     */
    @MXBeanDescription("The number of transactions which were rollback.")
    public long getTxRolledBackNum();

    /**
     * The number of active transactions holding at least one key lock.
     *
     * @return number of active transactions holding at least one key lock.
     */
    @MXBeanDescription("The number of active transactions holding at least one key lock.")
    public long getTxHoldingLockNum();

    /**
     *  The number of keys locked on the node.
     *
     * @return number of keys locked on the node.
     */
    @MXBeanDescription("The number of keys locked on the node.")
    public long getLockedKeysNum();

    /**
     * The number of active transactions for which this node is the initiator.
     *
     * @return number of active transactions for which this node is the initiator.
     */
    @MXBeanDescription("The number of active transactions for which this node is the initiator.")
    public long getOwnerTxNum();
}
