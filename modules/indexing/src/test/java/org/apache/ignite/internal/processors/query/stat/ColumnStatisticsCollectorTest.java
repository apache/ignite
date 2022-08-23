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
import java.time.LocalDate;
import java.time.Month;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test different scenario with column statistics collection.
 */
public class ColumnStatisticsCollectorTest extends GridCommonAbstractTest {
    /** Types with its comparators for tests.  */
    private static final Map<Class<?>, Object[]> types = new HashMap<>();

    /** */
    private static final Object[] ZERO_ARR = new Object[0];

    static {
        types.put(Boolean.class, new Object[]{false, true});
        types.put(Integer.class, new Object[]{1, 2, 10});
        types.put(Short.class, new Object[]{(short)1, (short)3});
        types.put(String.class, new Object[]{"1", "9"});
        types.put(BigDecimal.class, new Object[]{BigDecimal.ONE, BigDecimal.TEN});
        types.put(LocalDate.class, new Object[]{
            LocalDate.of(1945, Month.MAY, 9),
            LocalDate.of(1957, Month.OCTOBER, 4),
            LocalDate.of(1961, Month.APRIL, 12),
        });
        types.put(UUID.class, new Object[]{new UUID(1, 2), new UUID(2, 1), new UUID(2, 2)});
        types.put(Float.class, new Object[]{1f, 10f});
        types.put(Double.class, new Object[]{1., 10.});
        types.put(Byte.class, new Object[]{(byte)1, (byte)2});
    }

    /**
     * Test different types statistics collection, but without values.
     * Check that statistics collected properly.
     */
    @Test
    public void testZeroAggregation() throws Exception {
        for (Map.Entry<Class<?>, Object[]> tv: types.entrySet())
            testAggregation(tv.getKey(), 0, ZERO_ARR);
    }

    /**
     * Test statistics collection with only one null value.
     * Check that statistics collected properly.
     */
    @Test
    public void testSingleNullAggregation() throws Exception {
        for (Map.Entry<Class<?>, Object[]> tv: types.entrySet())
            testAggregation(tv.getKey(), 1);
    }

    /**
     * Test statistics collection with multiple null values.
     * Check that statistics collected properly.
     */
    @Test
    public void testMultipleNullsAggregation() throws Exception {
        for (Map.Entry<Class<?>, Object[]> tv: types.entrySet())
            testAggregation(tv.getKey(), 1000, ZERO_ARR);
    }

    /**
     * Test statistics collection with multiple values.
     * Check that statistics collected properly.
     */
    @Test
    public void testSingleAggregation() throws Exception {
        for (Map.Entry<Class<?>, Object[]> tv: types.entrySet()) {
            for (Object val : tv.getValue())
                testAggregation(tv.getKey(), 0, val);
        }
    }

    /**
     * Test statistics collection with multiple values.
     * Check that statistics collected properly.
     */
    @Test
    public void testMultipleAggregation() throws Exception {
        for (Map.Entry<Class<?>, Object[]> tv: types.entrySet())
            testAggregation(tv.getKey(), 0, tv.getValue());
    }

    /**
     * Test statistics collection with multiple values and nulls.
     * Check that statistics collected properly.
     */
    @Test
    public void testMultipleWithNullsAggregation() throws Exception {
        for (Map.Entry<Class<?>, Object[]> tv: types.entrySet())
            testAggregation(tv.getKey(), tv.getValue().length, tv.getValue());
    }

    /**
     * Test aggregation with specified values.
     * Check that statistics collected properly.
     *
     * @param type Value type.
     * @param nulls Nulls count.
     * @param vals Values to aggregate where the first one is the smallest and the last one is the biggest one.
     */
    private static void testAggregation(Class<?> type, int nulls, Object... vals) throws Exception {
        ColumnStatisticsCollector collector = new ColumnStatisticsCollector(0, "test", type);
        ColumnStatisticsCollector collectorInverted = new ColumnStatisticsCollector(0, "test", type);

        for (int i = 0; i < vals.length; i++) {
            collector.add(vals[i]);
            collectorInverted.add(vals[vals.length - 1 - i]);
        }
        for (int i = 0; i < nulls; i++) {
            collector.add(null);
            collectorInverted.add(null);
        }

        ColumnStatistics res = collector.finish();
        ColumnStatistics resInverted = collectorInverted.finish();

        testAggregationResult(type, res, nulls, vals);
        testAggregationResult(type, resInverted, nulls, vals);
    }

    /**
     * Check column statistics collection results.
     *
     * @param type Field type.
     * @param res Column statistics to test.
     * @param nulls Count of null values in statistics.
     * @param vals Values included into statistics where first one is the smallest one and the last one is the biggest.
     */
    private static void testAggregationResult(Class<?> type, ColumnStatistics res, int nulls, Object... vals) {
        if (vals.length == 0) {
            assertNull(res.min());
            assertNull(res.max());
        }
        else if (!type.isAssignableFrom(String.class)) {
            assertEquals(StatisticsUtils.toDecimal(vals[0]), res.min());
            assertEquals(StatisticsUtils.toDecimal(vals[vals.length - 1]), res.max());
        }

        assertEquals(nulls, res.nulls());

        int distinct = (vals.length == 0) ? 0 : new HashSet<>(Arrays.asList(vals)).size();

        assertEquals(distinct, res.distinct());
        assertEquals(vals.length + nulls, res.total());
        assertNotNull(res.raw());
    }
}
