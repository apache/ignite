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

import java.time.OffsetDateTime;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.metric.MetricRegistry;
import org.apache.ignite.spi.metric.counter.LongCounter;

import static org.apache.ignite.internal.metric.IoStatisticsType.CACHE_GROUP;

/**
 * Cache statistics holder to gather statistics related to concrete cache.
 */
public class IoStatisticsHolderCache implements IoStatisticsHolder {
    /** */
    public static final String PHYSICAL_READS = "PHYSICAL_READS";

    /** */
    public static final String LOGICAL_READS = "LOGICAL_READS";

    /** */
    private final LongCounter logicalReadCtr;

    /** */
    private final LongCounter physicalReadCtr;

    /** */
    private final String cacheName;

    /** */
    private final int grpId;

    /**
     * @param cacheName Name of cache.
     * @param grpId Group id.
     * @param mreg Metric registry.
     */
    public IoStatisticsHolderCache(String cacheName, int grpId, MetricRegistry mreg) {
        this.cacheName = cacheName;
        this.grpId = grpId;

        MetricRegistry mset = mreg.withPrefix(CACHE_GROUP.monitoringGroup(), cacheName);

        mset.objectGauge("startTime", OffsetDateTime.class, "").value(OffsetDateTime.now());
        mset.objectGauge("name", String.class, "").value(cacheName);
        mset.intGauge("grpId", "").value(grpId);

        this.logicalReadCtr = mset.counter(LOGICAL_READS, "");
        this.physicalReadCtr = mset.counter(PHYSICAL_READS, "");
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
