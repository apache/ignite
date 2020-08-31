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

import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.metric.IoStatisticsType.CACHE_GROUP;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Cache statistics holder to gather statistics related to concrete cache.
 */
public class IoStatisticsHolderCache implements IoStatisticsHolder {
    /** */
    public static final String PHYSICAL_READS = "PHYSICAL_READS";

    /** */
    public static final String LOGICAL_READS = "LOGICAL_READS";

    /** */
    private final LongAdderMetric logicalReadCtr;

    /** */
    private final LongAdderMetric physicalReadCtr;

    /** */
    private final String grpName;

    /** */
    private final int grpId;

    /**
     * @param grpName Name of the group.
     * @param grpId Group id.
     * @param mmgr Metric manager.
     */
    public IoStatisticsHolderCache(String grpName, int grpId, GridMetricManager mmgr) {
        assert grpName != null;

        this.grpName = grpName;
        this.grpId = grpId;

        MetricRegistry mreg = mmgr.registry(metricRegistryName());

        mreg.longMetric("startTime", null).value(U.currentTimeMillis());
        mreg.objectMetric("name", String.class, null).value(grpName);
        mreg.intMetric("grpId", null).value(grpId);

        this.logicalReadCtr = mreg.longAdderMetric(LOGICAL_READS, null);
        this.physicalReadCtr = mreg.longAdderMetric(PHYSICAL_READS, null);
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
        return logicalReadCtr.value();
    }

    /** {@inheritDoc} */
    @Override public long physicalReads() {
        return physicalReadCtr.value();
    }

    /** {@inheritDoc} */
    @Override public String metricRegistryName() {
        return metricName(CACHE_GROUP.metricGroupName(), grpName);
    }

    /**
     * @return Cache group id.
     */
    public int cacheGroupId() {
        return grpId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IoStatisticsHolderCache.class, this,
            "logicalReadCtr", logicalReadCtr,
            "physicalReadCtr", physicalReadCtr,
            "grpName", grpName);
    }
}
