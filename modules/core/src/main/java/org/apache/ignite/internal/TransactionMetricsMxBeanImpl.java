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

package org.apache.ignite.internal;

import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.mxbean.TransactionMetricsMxBean;
import org.apache.ignite.transactions.TransactionMetrics;

/**
 * Transactions MXBean implementation.
 */
public class TransactionMetricsMxBeanImpl implements TransactionMetricsMxBean {
    /** */
    private final TransactionMetrics transactionMetrics;

    /**
     * @param transactionMetrics Transaction metrics.
     */
    TransactionMetricsMxBeanImpl(TransactionMetrics transactionMetrics) {
        this.transactionMetrics = transactionMetrics;
    }

    /** {@inheritDoc} */
    @Override public long commitTime() {
        return transactionMetrics.commitTime();
    }

    /** {@inheritDoc} */
    @Override public long rollbackTime() {
        return transactionMetrics.rollbackTime();
    }

    /** {@inheritDoc} */
    @Override public int txCommits() {
        return transactionMetrics.txCommits();
    }

    /** {@inheritDoc} */
    @Override public int txRollbacks() {
        return transactionMetrics.txRollbacks();
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> getAllNearTxs() {
        return transactionMetrics.getAllNearTxs();
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> getLongRunningNearTxs(int duration) {
        return transactionMetrics.getLongRunningNearTxs(duration);
    }

    /** {@inheritDoc} */
    @Override public long getTxCommittedNum() {
        return transactionMetrics.getTxCommittedNum();
    }

    /** {@inheritDoc} */
    @Override public long getTxRolledBackNum() {
        return transactionMetrics.getTxRolledBackNum();
    }

    /** {@inheritDoc} */
    @Override public long getTxHoldingLockNum() {
        return transactionMetrics.getTxHoldingLockNum();
    }

    /** {@inheritDoc} */
    @Override public long getLockedKeysNum() {
        return transactionMetrics.getLockedKeysNum();
    }

    /** {@inheritDoc} */
    @Override public long getOwnerTxNum() {
        return transactionMetrics.getOwnerTxNum();
    }
}


