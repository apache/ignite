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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetricImpl;

/**
 * Index statistics holder to gather statistics related to concrete index.
 */
public class IoStatisticsHolderIndex implements IoStatisticsHolder {
    /** Display name of hash PK index. */
    public static final String HASH_PK_IDX_NAME = "HASH_PK";

    /** */
    public static final String LOGICAL_READS_LEAF = "LOGICAL_READS_LEAF";

    /** */
    public static final String LOGICAL_READS_INNER = "LOGICAL_READS_INNER";

    /** */
    public static final String PHYSICAL_READS_LEAF = "PHYSICAL_READS_LEAF";

    /** */
    public static final String PHYSICAL_READS_INNER = "PHYSICAL_READS_INNER";

    /** */
    private final LongAdderMetricImpl logicalReadLeafCtr;

    /** */
    private final LongAdderMetricImpl logicalReadInnerCtr;

    /** */
    private final LongAdderMetricImpl physicalReadLeafCtr;

    /** */
    private final LongAdderMetricImpl physicalReadInnerCtr;

    /** */
    private final String cacheName;

    /** */
    private final String idxName;

    /**
     * @param type Type of statistics.
     * @param cacheName Cache name.
     * @param idxName Index name.
     * @param mreg Metric registry.
     */
    public IoStatisticsHolderIndex(
        IoStatisticsType type,
        String cacheName,
        String idxName,
        MetricRegistry mreg) {
        assert cacheName != null && idxName != null;

        this.cacheName = cacheName;
        this.idxName = idxName;

        MetricRegistry mset = mreg.withPrefix(type.metricGroupName(), cacheName, idxName);

        mset.metric("startTime", null).value(U.currentTimeMillis());
        mset.objectMetric("name", String.class, null).value(cacheName);
        mset.objectMetric("indexName", String.class, null).value(idxName);

        logicalReadLeafCtr = mset.longAdderMetric(LOGICAL_READS_LEAF, null);
        logicalReadInnerCtr = mset.longAdderMetric(LOGICAL_READS_INNER, null);
        physicalReadLeafCtr = mset.longAdderMetric(PHYSICAL_READS_LEAF, null);
        physicalReadInnerCtr = mset.longAdderMetric(PHYSICAL_READS_INNER, null);
    }

    /** {@inheritDoc} */
    @Override public void trackLogicalRead(long pageAddr) {
        IndexPageType idxPageType = PageIO.deriveIndexPageType(pageAddr);

        switch (idxPageType) {
            case INNER:
                logicalReadInnerCtr.increment();

                IoStatisticsQueryHelper.trackLogicalReadQuery(pageAddr);

                break;

            case LEAF:
                logicalReadLeafCtr.increment();

                IoStatisticsQueryHelper.trackLogicalReadQuery(pageAddr);

                break;
        }

    }

    /** {@inheritDoc} */
    @Override public void trackPhysicalAndLogicalRead(long pageAddr) {
        IndexPageType idxPageType = PageIO.deriveIndexPageType(pageAddr);

        switch (idxPageType) {
            case INNER:
                logicalReadInnerCtr.increment();
                physicalReadInnerCtr.increment();

                IoStatisticsQueryHelper.trackPhysicalAndLogicalReadQuery(pageAddr);

                break;

            case LEAF:
                logicalReadLeafCtr.increment();
                physicalReadLeafCtr.increment();

                IoStatisticsQueryHelper.trackPhysicalAndLogicalReadQuery(pageAddr);

                break;
        }
    }

    /** {@inheritDoc} */
    @Override public long logicalReads() {
        return logicalReadLeafCtr.longValue() + logicalReadInnerCtr.longValue();
    }

    /** {@inheritDoc} */
    @Override public long physicalReads() {
        return physicalReadLeafCtr.longValue() + physicalReadInnerCtr.longValue();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IoStatisticsHolderIndex.class, this,
            "logicalReadLeafCtr", logicalReadLeafCtr,
            "logicalReadInnerCtr", logicalReadInnerCtr,
            "physicalReadLeafCtr", physicalReadLeafCtr,
            "physicalReadInnerCtr", physicalReadInnerCtr,
            "cacheName", cacheName,
            "idxName", idxName);
    }
}
