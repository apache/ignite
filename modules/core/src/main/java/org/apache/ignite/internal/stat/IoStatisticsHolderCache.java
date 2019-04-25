/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.stat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Cache statistics holder to gather statistics related to concrete cache.
 */
public class IoStatisticsHolderCache implements IoStatisticsHolder {
    /** */
    private static final String PHYSICAL_READS = "PHYSICAL_READS";

    /** */
    private static final String LOGICAL_READS = "LOGICAL_READS";

    /** */
    private LongAdder logicalReadCtr = new LongAdder();

    /** */
    private LongAdder physicalReadCtr = new LongAdder();

    /** */
    private final String cacheName;

    /** */
    private final int grpId;

    /**
     * @param cacheName Name of cache.
     */
    public IoStatisticsHolderCache(String cacheName, int grpId) {
        assert cacheName != null;

        this.cacheName = cacheName;
        this.grpId = grpId;
    }

    /** {@inheritDoc} */
    @Override public void trackLogicalRead(long pageAddr) {
        int pageIoType = PageIO.getType(pageAddr);

        if (pageIoType == PageIO.T_DATA) {
            logicalReadCtr.increment();

            IoStatisticsQueryHelper.trackLogicalReadQuery(pageAddr);
        }

    }

    /** {@inheritDoc} */
    @Override public void trackPhysicalAndLogicalRead(long pageAddr) {
        int pageIoType = PageIO.getType(pageAddr);

        if (pageIoType == PageIO.T_DATA) {
            logicalReadCtr.increment();

            physicalReadCtr.increment();

            IoStatisticsQueryHelper.trackPhysicalAndLogicalReadQuery(pageAddr);
        }
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
        logicalReadCtr.reset();
        physicalReadCtr.reset();
    }

    /**
     * @return Cache group id.
     */
    public int cacheGroupId(){
        return grpId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IoStatisticsHolderCache.class, this,
            "logicalReadCtr", logicalReadCtr,
            "physicalReadCtr", physicalReadCtr,
            "cacheName", cacheName);
    }
}
