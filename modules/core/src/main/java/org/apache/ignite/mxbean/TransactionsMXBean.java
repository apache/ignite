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

    /**
     * Shows if dump requests from local node to near node are allowed, when long running transaction
     * is found. If allowed, the compute request to near node will be made to get thread dump of transaction
     * owner thread.
     *
     * @return <code>true</code> if allowed, <code>false</code> otherwise.
     */
    @MXBeanDescription(
        "Shows if dump requests from local node to near node are allowed, " +
        "when long running transaction  is found. If allowed, the compute request to near " +
        "node will be made to get thread dump of transaction owner thread."
    )
    public boolean getTxOwnerDumpRequestsAllowed();

    /**
     * Sets if dump requests from local node to near node are allowed, when long running transaction
     * is found. If allowed, the compute request to near node will be made to get thread dump of transaction
     * owner thread.
     *
     * @param allowed whether to allow
     */
    @MXBeanDescription(
        "Shows if dump requests from local node to near node are allowed, " +
        "when long running transaction  is found. If allowed, the compute request to near " +
        "node will be made to get thread dump of transaction owner thread."
    )
    @MXBeanParametersNames("allowed")
    @MXBeanParametersDescriptions(
        "whether to allow"
    )
    public void setTxOwnerDumpRequestsAllowed(boolean allowed);

    /**
     * Returns threshold timeout in milliseconds for long transactions, if transaction exceeds it,
     * it will be dumped in log with information about how much time did
     * it spent in system time (time while aquiring locks, preparing, commiting, etc.)
     * and user time (time when client node runs some code while holding transaction).
     * Returns 0 if not set. No transactions are dumped in log if this parameter is not set.
     *
     * @return Threshold.
     */
    @MXBeanDescription(
        "Returns threshold timeout in milliseconds for long transactions, if transaction exceeds it, " +
        "it will be dumped in log with information about how much time did " +
        "it spent in system time (time while aquiring locks, preparing, commiting, etc.)" +
        "and user time (time when client node runs some code while holding transaction). " +
        "Returns 0 if not set. No transactions are dumped in log if this parameter is not set."
    )
    public long getLongTransactionTimeDumpThreshold();

    /**
     * Sets threshold timeout in milliseconds for long transactions, if transaction exceeds it,
     * it will be dumped in log with information about how much time did
     * it spent in system time (time while aquiring locks, preparing, commiting, etc.)
     * and user time (time when client node runs some code while holding transaction).
     * Can be set to 0 - no transactions will be dumped in log in this case.
     *
     * @param threshold Threshold.
     */
    @MXBeanDescription(
        "Sets threshold timeout in milliseconds for long transactions, if transaction exceeds it, " +
        "it will be dumped in log with information about how much time did " +
        "it spent in system time (time while aquiring locks, preparing, commiting, etc.) " +
        "and user time (time when client node runs some code while holding transaction). " +
        "Can be set to 0 - no transactions will be dumped in log in this case."
    )
    @MXBeanParametersNames("threshold")
    @MXBeanParametersDescriptions("threshold timeout")
    public void setLongTransactionTimeDumpThreshold(long threshold);

    /**
     * Returns the coefficient for samples of completed transactions that will be dumped in log.
     *
     * @return Coefficient current value.
     */
    @MXBeanDescription(
        "Returns the coefficient for samples of completed transactions that will be dumped in log."
    )
    public double getTransactionTimeDumpSamplesCoefficient();

    /**
     * Sets the coefficient for samples of completed transactions that will be dumped in log.
     *
     * @param coefficient Coefficient.
     */
    @MXBeanDescription(
        "Sets the coefficient for samples of completed transactions that will be dumped in log."
    )
    @MXBeanParametersNames("coefficient")
    @MXBeanParametersDescriptions("Samples coefficient.")
    public void setTransactionTimeDumpSamplesCoefficient(double coefficient);

    /**
     * Returns the limit of samples of completed transactions that will be dumped in log per second,
     * if {@link #getTransactionTimeDumpSamplesCoefficient} is above <code>0.0</code>.
     * Must be integer value greater than <code>0</code>.
     *
     * @return Limit value.
     */
    @MXBeanDescription(
        "Returns the limit of samples of completed transactions that will be dumped in log per second, " +
        "if {@link #getTransactionTimeDumpSamplesCoefficient} is above <code>0.0</code>. " +
        "Must be integer value greater than <code>0</code>."
    )
    public int getTransactionTimeDumpSamplesPerSecondLimit();

    /**
     * Sets the limit of samples of completed transactions that will be dumped in log per second,
     * if {@link #getTransactionTimeDumpSamplesCoefficient} is above <code>0.0</code>.
     * Must be integer value greater than <code>0</code>.
     *
     * @param limit Limit value.
     */
    @MXBeanDescription(
        "Sets the limit of samples of completed transactions that will be dumped in log per second, " +
        "if {@link #getTransactionTimeDumpSamplesCoefficient} is above <code>0.0</code>. " +
        "Must be integer value greater than <code>0</code>."
    )
    @MXBeanParametersNames("limit")
    @MXBeanParametersDescriptions("Samples per second limit.")
    public void setTransactionTimeDumpSamplesPerSecondLimit(int limit);

    /**
     * Setting a timeout (in millis) for printing long-running transactions as
     * well as transactions that cannot receive locks for all their keys for a
     * long time. Set less than or equal {@code 0} to disable.
     *
     * @param timeout Timeout.
     */
    @MXBeanDescription(
        "Setting a timeout (in millis) for printing long-running transactions as well as transactions that cannot " +
            "receive locks for all their keys for a long time. Set less than or equal {@code 0} to disable."
    )
    @MXBeanParametersNames("timeout")
    @MXBeanParametersDescriptions("Long operations dump timeout.")
    void setLongOperationsDumpTimeout(long timeout);

    /**
     * Returns a timeout (in millis) for printing long-running transactions as
     * well as transactions that cannot receive locks for all their keys for a
     * long time. Returns {@code 0} or less if not set.
     *
     * @return Timeout.
     */
    @MXBeanDescription(
        "Returns a timeout (in millis) for printing long-running transactions as well as transactions that cannot " +
            "receive locks for all their keys for a long time. Returns {@code 0} or less if not set."
    )
    long getLongOperationsDumpTimeout();
}
