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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Closure that is sent on all server nodes in order to change configuration parameters
 * of dumping long running transactions' system and user time values.
 */
public class LongRunningTxTimeDumpSettingsClosure implements IgniteRunnable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final Long timeoutThreshold;

    /** */
    private final Double samplesCoefficient;

    /** */
    private final Integer samplesPerSecondLimit;

    /**
     * Auto-inject Ignite instance
     */
    @IgniteInstanceResource
    private Ignite ignite;

    /** */
    public LongRunningTxTimeDumpSettingsClosure(
        Long timeoutThreshold,
        Double samplesCoefficient,
        Integer samplesPerSecondLimit
    ) {
        this.timeoutThreshold = timeoutThreshold;
        this.samplesCoefficient = samplesCoefficient;
        this.samplesPerSecondLimit = samplesPerSecondLimit;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        IgniteTxManager tm = ((IgniteEx) ignite).context().cache().context().tm();

        if (timeoutThreshold != null)
            tm.longTransactionTimeDumpThreshold(timeoutThreshold);

        if (samplesCoefficient != null)
            tm.transactionTimeDumpSamplesCoefficient(samplesCoefficient);

        if (samplesPerSecondLimit != null)
            tm.transactionTimeDumpSamplesPerSecondLimit(samplesPerSecondLimit);
    }
}
