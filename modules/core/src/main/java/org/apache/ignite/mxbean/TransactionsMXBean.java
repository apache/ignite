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

import org.apache.ignite.configuration.TransactionConfiguration;

/**
 * Transactions MXBean interface.
 */
@MXBeanDescription("MBean that provides access to Ignite transactions.")
public interface TransactionsMXBean {
    /**
     * @param minDuration Minimum duration.
     * @param minSize Minimum size.
     * @param prj Projection.
     * @param consistentIds Consistent ids.
     * @param xid Xid.
     * @param lbRegex Label regex.
     * @param limit Limit.
     * @param order Order.
     * @param detailed Detailed.
     * @param kill Kill.
     */
    @MXBeanDescription("Returns or kills transactions matching the filter conditions.")
    @MXBeanParametersNames(
        {
            "minDuration",
            "minSize",
            "prj",
            "consistentIds",
            "xid",
            "lbRegex",
            "limit",
            "order",
            "detailed",
            "kill"
        }
    )
    @MXBeanParametersDescriptions(
        {
            "Minimum duration (seconds).",
            "Minimum size.",
            "Projection (servers|clients).",
            "Consistent ids (separated by comma).",
            "Transaction XID.",
            "Label regexp.",
            "Limit a number of transactions collected on each node.",
            "Order by DURATION|SIZE.",
            "Show detailed description, otherwise only count.",
            "Kill matching transactions (be careful)."
        }
    )
    public String getActiveTransactions(Long minDuration, Integer minSize, String prj,
        String consistentIds, String xid, String lbRegex, Integer limit, String order, boolean detailed, boolean kill);

    /**
     * Gets transaction timeout on partition map exchange.
     * <p>
     * @see TransactionConfiguration#getTxTimeoutOnPartitionMapExchange
     *
     * @return Transaction timeout on partition map exchange in milliseconds.
     */
    @MXBeanDescription("Returns transaction timeout on partition map exchange in milliseconds.")
    public long getTxTimeoutOnPartitionMapExchange();

    /**
     * Sets transaction timeout on partition map exchange.
     * <p>
     * If not set, default value is {@link TransactionConfiguration#TX_TIMEOUT_ON_PARTITION_MAP_EXCHANGE} which means
     * transactions will never be rolled back on partition map exchange.
     * <p>
     * @see TransactionConfiguration#setTxTimeoutOnPartitionMapExchange
     *
     * @param timeout Transaction timeout on partition map exchange in milliseconds.
     */
    @MXBeanDescription("Sets transaction timeout on partition map exchange in milliseconds.")
    @MXBeanParametersNames(
        "timeout"
    )
    @MXBeanParametersDescriptions(
        "Transaction timeout on partition map exchange in milliseconds."
    )
    public void setTxTimeoutOnPartitionMapExchange(long timeout);
}
