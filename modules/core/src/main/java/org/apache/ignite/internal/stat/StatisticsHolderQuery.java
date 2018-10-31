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

package org.apache.ignite.internal.stat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * Query Statistics holder to gather statistics related to concrete query.
 * Used in {@code org.apache.ignite.internal.stat.StatisticsHolderIndex} and {@code org.apache.ignite.internal.stat.StatisticsHolderCache}.
 * Query Statistics holder to gather statistics related to concrete query. Used in {@code
 * org.apache.ignite.internal.stat.StatisticsHolderIndex} and {@code org.apache.ignite.internal.stat.StatisticsHolderCache}.
 */
public class StatisticsHolderQuery implements StatisticsHolder {
    /** */
    public static final String PHYSICAL_READS = "PHYSICAL_READS";
    /** */
    public static final String LOGICAL_READS = "LOGICAL_READS";
    /** */
    private LongAdder logicalReadCntr = new LongAdder();

    /** */
    private LongAdder physicalReadCntr = new LongAdder();

    /** */
    private final String qryId;

    /**
     * @param qryId Query id.
     */
    public StatisticsHolderQuery(String qryId) {
        this.qryId = qryId;
    }

    /** {@inheritDoc} */
    @Override public void trackLogicalRead(long pageAddr) {
        logicalReadCntr.increment();
    }

    /** {@inheritDoc} */
    @Override public void trackPhysicalAndLogicalRead(long pageAddr) {
        logicalReadCntr.increment();

        physicalReadCntr.increment();
    }

    /** {@inheritDoc} */
    @Override public long logicalReads() {
        return logicalReadCntr.longValue();
    }

    /** {@inheritDoc} */
    @Override public long physicalReads() {
        return physicalReadCntr.longValue();
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> logicalReadsMap() {
        Map<String, Long> res = new HashMap<>(2);

        res.put(LOGICAL_READS, logicalReads());

        return res;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> physicalReadsMap() {
        Map<String, Long> res = new HashMap<>(2);

        res.put(PHYSICAL_READS, physicalReads());

        return res;
    }

    /** {@inheritDoc} */
    @Override public void resetStatistics() {
        logicalReadCntr.reset();

        physicalReadCntr.reset();
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
        logicalReadCntr.add(logicalReads);

        physicalReadCntr.add(physicalReads);
    }
}
