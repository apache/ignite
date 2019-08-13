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
package org.apache.ignite.internal.processors.metrics.impl;

import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.toJson;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class MetricUtilsTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testToJson() {
        HistogramMetric singleBucket = new HistogramMetric(new long[] { 1 });

        singleBucket.value(1);

        String singleBucketJson = toJson(singleBucket);

        assertEquals("{\"bounds\":[1],\"values\":" +
                "[{\"fromExclusive\":0,\"toInclusive\":1,\"value\":1},{\"fromExclusive\":1,\"value\":0}]}",
            singleBucketJson
        );

        HistogramMetric histo = new HistogramMetric(new long[] { 1, 5, 10, 20, 50 });

        histo.value(1);
        histo.value(12);
        histo.value(13);
        histo.value(35);
        histo.value(100);

        String histoJson = toJson(histo);

        assertEquals("{\"bounds\":[1,5,10,20,50],\"values\":" +
                "[{\"fromExclusive\":0,\"toInclusive\":1,\"value\":1}," +
                "{\"fromExclusive\":1,\"toInclusive\":5,\"value\":0}," +
                "{\"fromExclusive\":5,\"toInclusive\":10,\"value\":0}," +
                "{\"fromExclusive\":10,\"toInclusive\":20,\"value\":2}," +
                "{\"fromExclusive\":20,\"toInclusive\":50,\"value\":1}," +
                "{\"fromExclusive\":50,\"value\":1}]}",
            histoJson
        );
    }
}
