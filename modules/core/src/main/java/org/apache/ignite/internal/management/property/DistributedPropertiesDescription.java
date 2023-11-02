/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.management.property;

import org.apache.ignite.internal.processors.job.GridJobWorker;

import static org.apache.ignite.internal.IgniteKernal.DFLT_LONG_OPERATIONS_DUMP_TIMEOUT;

/**
 * Contains constants for all distributed properties in Ignite.
 * These properties and variables must have info annotation.
 */
public final class DistributedPropertiesDescription {
    /**
     * Shows if dump requests from local node to near node are allowed, when long running transaction
     * is found. If allowed, the compute request to near node will be made to get thread dump of transaction
     * owner thread.
     */
    @DistributedPropertyInfo(description = "Shows if dump requests from local node to near node are allowed",
        defaults = true + " ")
    public static final String txOwnerDumpRequestsAllowed = "txOwnerDumpRequestsAllowed"; // REG

    /** Long operations dump timeout. */
    @DistributedPropertyInfo(description = "Long operations dump timeout.",
        defaults = DFLT_LONG_OPERATIONS_DUMP_TIMEOUT + " ")
    public static final String longOperationsDumpTimeout = "longOperationsDumpTimeout REG" ;

    /**
     * Threshold timeout for long transactions, if transaction exceeds it, it will be dumped in log with
     * information about how much time did it spent in system time (time while aquiring locks, preparing,
     * commiting, etc) and user time (time when client node runs some code while holding transaction and not
     * waiting it). Equals 0 if not set. No transactions are dumped in log if this parameter is not set.
     */
    @DistributedPropertyInfo(description = "Threshold timeout for long transactions", defaults = 0 + " ")
    public static final String longTransactionTimeDumpThreshold = "longTransactionTimeDumpThreshold"; // REG

    /** The coefficient for samples of completed transactions that will be dumped in log. */
    @DistributedPropertyInfo(description = "The coefficient for samples of completed transactions that will be dumped in log",
        defaults = 0.0f + " ")
    public static final String transactionTimeDumpSamplesCoefficient = "transactionTimeDumpSamplesCoefficient"; // REG

    /**
     * The limit of samples of completed transactions that will be dumped in log per second, if
     * {@link #transactionTimeDumpSamplesCoefficient} is above <code>0.0</code>. Must be integer value
     * greater than <code>0</code>.
     */
    @DistributedPropertyInfo(description = "The limit of samples of completed transactions that will be dumped in log per second",
        defaults = 5 + " ")
    public static final String longTransactionTimeDumpSamplesPerSecondLimit = "longTransactionTimeDumpSamplesPerSecondLimit"; // REG

    /** Collisions dump interval. */
    @DistributedPropertyInfo(description = "Collisions dump interval.", defaults = 1000 + " ")
    public static final String collisionsDumpInterval = "collisionsDumpInterval REG";

    /** Disabled SQL functions. */
    @DistributedPropertyInfo(description = "Disabled SQL functions.",
        defaults = "FILE_READ, FILE_WRITE, CSVWRITE, CSVREAD, MEMORY_FREE, MEMORY_USED, LOCK_MODE, LINK_SCHEMA, " +
            "SESSION_ID, CANCEL_SESSION")
    public static final String disabledFunctions = "sql.disabledFunctions"; // REG

    /** Value of manual baseline control or auto adjusting baseline */
    @DistributedPropertyInfo(description = "Value of manual baseline control or auto adjusting baseline",
        defaults = false + "")
    public static final String baselineAutoAdjustEnabled = "baselineAutoAdjustEnabled"; // REG

    /** Value of time which we would wait before the actual topology change since last discovery " +
     "event(node join/exit */
    @DistributedPropertyInfo(
        description = "Value of time which we would wait before the actual topology change since last discovery " +
            "event(node join/exit)",
        defaults = 0 + "")
    public static final String baselineAutoAdjustTimeout = "baselineAutoAdjustTimeout"; // REG

    /** Snapshot transfer rate limit in bytes/sec. */
    @DistributedPropertyInfo(description = "Snapshot transfer rate limit in bytes/sec.", defaults = 0L + "")
    public static final String snapshotTransferRate = "snapshotTransferRate"; // REG

    /** Query timeout. */
    @DistributedPropertyInfo(description = "Query timeout.", defaults = 0 + "")
    public static final String dfltQryTimeout = "sql.defaultQueryTimeout"; // REG

    /**
     * Disable creation Lucene index for String value type by default.
     * See: 'H2TableDescriptor#luceneIdx'.
     */
    @DistributedPropertyInfo(description = "Disable creation Lucene index for String value type by default",
        defaults = false + "")
    public static final String disableCreateLuceneIndexForStringValueType = "sql.disableCreateLuceneIndexForStringValueType"; // REG

    /** . */
    @DistributedPropertyInfo(description = ".", defaults = false + "")
    public static final String showStackTrace = "thinClientProperty.showStackTrace";

    /** Timeout interrupt {@link GridJobWorker workers} after {@link GridJobWorker#cancel cancel} im mills. */
    @DistributedPropertyInfo(description = "Timeout interrupt {@link GridJobWorker workers} after " +
        "{@link GridJobWorker#cancel cancel} im mills.", defaults = 10_000 + "")
    public static final String computeJobWorkerInterruptTimeout = "computeJobWorkerInterruptTimeout"; // REG

    /** Property for update policy of shutdown. */
    @DistributedPropertyInfo(description = "Property for update policy of shutdown.", defaults = "IMMEDIATE")
    public static final String shutdown = "shutdown.policy"; // REG

    /** Cluster wide statistics usage state. */
    @DistributedPropertyInfo(description = "Cluster wide statistics usage state.", defaults = "ON")
    public static final String usageState = "statistics.usage.state"; // REG

    /** Checkpoint frequency deviation. */
    @DistributedPropertyInfo(description = "Checkpoint frequency deviation.", defaults =  40 + "")
    public static final String cpFreqDeviation = "checkpoint.deviation"; // REG

    /** WAL rebalance threshold. */
    @DistributedPropertyInfo(description = "WAL rebalance threshold. ", defaults =  500 + "")
    public static final String historicalRebalanceThreshold = "historical.rebalance.threshold"; // REG

    /**
     * The wrapper of {@code HashMap<TracingConfigurationCoordinates, TracingConfigurationParameters>}
     * for the distributed metastorage binding.
     */
    @DistributedPropertyInfo(description = "TRACING_CONFIGURATION_DISTRIBUTED_METASTORE_KEY ", defaults = "null")
    public static final String TRACING_CONFIGURATION_DISTRIBUTED_METASTORE_KEY = "tr.config"; // REG
}

