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

package org.apache.ignite.internal.processors.query.stat;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.ignite.internal.processors.query.stat.hll.HLL;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.value.Value;
import org.h2.value.ValueDecimal;
import org.junit.Test;

/**
 * Test different scenarious with column statistics aggregation.
 */
public class ColumnStatisticsCollectorAggregationTest extends GridCommonAbstractTest {
    /** Decimal comparator. */
    private static final Comparator<Value> DECIMAL_VALUE_COMPARATOR = (v1, v2) ->
        v1.getBigDecimal().compareTo(v2.getBigDecimal());

    /**
     * Aggregate single column statistics object.
     * Test that aggregated object are the same as original.
     */
    @Test
    public void aggregateSingleTest() {
        List<ColumnStatistics> statistics = new ArrayList<>();
        ColumnStatistics stat1 = new ColumnStatistics(null, null, 100, 0, 100, 0,
            getHLL(-1).toBytes(), 0, U.currentTimeMillis());
        statistics.add(stat1);

        ColumnStatistics res = ColumnStatisticsCollector.aggregate(DECIMAL_VALUE_COMPARATOR, statistics, null);

        assertEquals(stat1, res);
    }

    /**
     * Aggregate column statistics without values.
     * Test that they aggregated correctly.
     */
    @Test
    public void aggregateNullTest() {
        List<ColumnStatistics> statistics = new ArrayList<>();
        ColumnStatistics stat1 = new ColumnStatistics(null, null, 100, 0, 100, 0,
            getHLL(-1).toBytes(), 0, U.currentTimeMillis());
        ColumnStatistics stat2 = new ColumnStatistics(null, null, 100, 0, 10, 0,
            getHLL(-1).toBytes(), 0, U.currentTimeMillis());

        statistics.add(stat1);
        statistics.add(stat2);

        ColumnStatistics res = ColumnStatisticsCollector.aggregate(DECIMAL_VALUE_COMPARATOR, statistics, null);

        assertNull(res.min());
        assertNull(res.max());
        assertEquals(200, res.nulls());
        assertEquals(0, res.distinct());
        assertEquals(110, res.total());
        assertEquals(0, res.size());
        assertNotNull(res.raw());
    }

    /**
     * Test aggregation with real values.
     * Check that they aggregated correctly.
     */
    @Test
    public void aggregateTest() {
        List<ColumnStatistics> statistics = new ArrayList<>();
        ColumnStatistics stat1 = new ColumnStatistics(ValueDecimal.get(BigDecimal.ONE), ValueDecimal.get(BigDecimal.TEN),
            50, 10, 1000, 0, getHLL(50).toBytes(), 0, U.currentTimeMillis());
        ColumnStatistics stat2 = new ColumnStatistics(ValueDecimal.get(BigDecimal.ZERO), ValueDecimal.get(BigDecimal.ONE),
            10, 100, 10, 0, getHLL(9).toBytes(), 0, U.currentTimeMillis());

        statistics.add(stat1);
        statistics.add(stat2);

        ColumnStatistics res = ColumnStatisticsCollector.aggregate(DECIMAL_VALUE_COMPARATOR, statistics, null);

        assertEquals(ValueDecimal.get(BigDecimal.ZERO), res.min());
        assertEquals(ValueDecimal.get(BigDecimal.TEN), res.max());
        assertEquals(60, res.nulls());
        assertEquals(59, res.distinct());
        assertEquals(1010, res.total());
        assertEquals(0, res.size());
        assertNotNull(res.raw());
    }

    /**
     * Generate HLL with specified number of unique values.
     *
     * @param uniq Desired unique value count.
     * @return HLL with specified (or near) cardinality.
     */
    private HLL getHLL(int uniq) {
        HLL res = new HLL(13, 5);
        Random r = ThreadLocalRandom.current();
        for (int i = 0; i < uniq; i++)
            res.addRaw(r.nextLong());

        return res;
    }
}
