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
 *
 */

package org.apache.ignite.internal.metric;

import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Query Statistics holder to gather statistics related to concrete query.
 * Used in {@code org.apache.ignite.internal.stat.IoStatisticsHolderIndex} and {@code org.apache.ignite.internal.stat.IoStatisticsHolderCache}.
 * Query Statistics holder to gather statistics related to concrete query. Used in {@code
 * org.apache.ignite.internal.stat.IoStatisticsHolderIndex} and {@code org.apache.ignite.internal.stat.IoStatisticsHolderCache}.
 */
public class IoStatisticsHolderQuery implements IoStatisticsHolder {
    /** */
    public static final String PHYSICAL_READS = "PHYSICAL_READS";

    /** */
    public static final String LOGICAL_READS = "LOGICAL_READS";

    /** */
    private LongAdder logicalReadCtr = new LongAdder();

    /** */
    private LongAdder physicalReadCtr = new LongAdder();

    /** */
    private final String qryId;

    /**
     * @param qryId Query id.
     */
    public IoStatisticsHolderQuery(String qryId) {
        this.qryId = qryId;
    }

    /** {@inheritDoc} */
    @Override public void trackLogicalRead(long pageAddr) {
        logicalReadCtr.increment();
    }

    /** {@inheritDoc} */
    @Override public void trackPhysicalAndLogicalRead(long pageAddr) {
        logicalReadCtr.increment();

        physicalReadCtr.increment();
    }

    /** {@inheritDoc} */
    @Override public long logicalReads() {
        return logicalReadCtr.longValue();
    }

    /** {@inheritDoc} */
    @Override public long physicalReads() {
        return physicalReadCtr.longValue();
    }

    /** {@inheritDoc} */
    @Override public String metricRegistryName() {
        return null;
    }

    /**
     * @return Query id.
     */
    public String queryId() {
        return qryId;
    }

    /**
     * Add given given statistics into this.
     * Merge query statistics.
     *
     * @param logicalReads Logical reads which will be added to current query statistics.
     * @param physicalReads Physical reads which will be added to current query statistics,
     */
    public void merge(long logicalReads, long physicalReads) {
        logicalReadCtr.add(logicalReads);

        physicalReadCtr.add(physicalReads);
    }

    @Override public String toString() {
        return S.toString(IoStatisticsHolderQuery.class, this,
            "logicalReadCtr", logicalReadCtr,
            "physicalReadCtr", physicalReadCtr,
            "qryId", qryId);
    }
}
