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

package org.apache.ignite.internal.processors.metric.sources;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.metric.IndexPageType;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.metric.IoStatisticsQueryHelper;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.metric.MetricRegistryBuilder;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Metric source for indexes metrics.
 */
public class IndexMetricSource extends AbstractMetricSource<IndexMetricSource.Holder> implements IoStatisticsHolder {
    /** Display name of hash PK index. */
    public static final String HASH_PK_IDX_NAME = "HASH_PK";

    /** Hash index type name. */
    public static final String HASH_IDX = "hashIndex";

    /** Sorted index type name. */
    public static final String SORTED_IDX = "sortedIndex";

    /** */
    public static final String LOGICAL_READS_LEAF = "logicalReadsOnLeaf";

    /** */
    public static final String LOGICAL_READS_INNER = "logicalReadsOnInner";

    /** */
    public static final String PHYSICAL_READS_LEAF = "physicalReadsOnLeaf";

    /** */
    public static final String PHYSICAL_READS_INNER = "physicalReadsOnInner";

    /**
     * Creates metric source for index.
     *
     * @param pref Prefix.
     * @param cacheName Cache name.
     * @param idxName Index name.
     * @param ctx Kernal context.
     */
    public IndexMetricSource(String pref, String cacheName, String idxName, GridKernalContext ctx) {
        super(metricName(pref, cacheName, idxName), ctx);
    }

    //TODO: Add metrics descriptions
    /** {@inheritDoc} */
    @Override protected void init(MetricRegistryBuilder bldr, Holder hldr) {
        bldr.longMetric("startTime", "Index start time.").value(U.currentTimeMillis());

        hldr.logicalReadLeafCtr = bldr.longAdderMetric(LOGICAL_READS_LEAF, null);

        hldr.logicalReadInnerCtr = bldr.longAdderMetric(LOGICAL_READS_INNER, null);

        hldr.physicalReadLeafCtr = bldr.longAdderMetric(PHYSICAL_READS_LEAF, null);

        hldr.physicalReadInnerCtr = bldr.longAdderMetric(PHYSICAL_READS_INNER, null);
    }

    /** {@inheritDoc} */
    @Override public void trackLogicalRead(long pageAddr) {
        Holder hldr = holder();

        if (hldr != null) {
            IndexPageType idxPageType = PageIO.deriveIndexPageType(pageAddr);

            switch (idxPageType) {
                case INNER:
                    hldr.logicalReadInnerCtr.increment();

                    IoStatisticsQueryHelper.trackLogicalReadQuery(pageAddr);

                    break;

                case LEAF:
                    hldr.logicalReadLeafCtr.increment();

                    IoStatisticsQueryHelper.trackLogicalReadQuery(pageAddr);

                    break;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void trackPhysicalAndLogicalRead(long pageAddr) {
        Holder hldr = holder();

        if (hldr != null) {
            IndexPageType idxPageType = PageIO.deriveIndexPageType(pageAddr);

            switch (idxPageType) {
                case INNER:
                    hldr.logicalReadInnerCtr.increment();
                    hldr.physicalReadInnerCtr.increment();

                    IoStatisticsQueryHelper.trackPhysicalAndLogicalReadQuery(pageAddr);

                    break;

                case LEAF:
                    hldr.logicalReadLeafCtr.increment();
                    hldr.physicalReadLeafCtr.increment();

                    IoStatisticsQueryHelper.trackPhysicalAndLogicalReadQuery(pageAddr);

                    break;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public long logicalReads() {
        Holder hldr = holder();

        return hldr != null ? hldr.logicalReadLeafCtr.value() + hldr.logicalReadInnerCtr.value() : -1;
    }

    /** {@inheritDoc} */
    @Override public long physicalReads() {
        Holder hldr = holder();

        return hldr != null ? hldr.physicalReadLeafCtr.value() + hldr.physicalReadInnerCtr.value() : -1;
    }

    /** {@inheritDoc} */
    @Override protected Holder createHolder() {
        return new Holder();
    }

    /** */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        /** */
        private LongAdderMetric logicalReadLeafCtr;

        /** */
        private LongAdderMetric logicalReadInnerCtr;

        /** */
        private LongAdderMetric physicalReadLeafCtr;

        /** */
        private LongAdderMetric physicalReadInnerCtr;
    }
}
