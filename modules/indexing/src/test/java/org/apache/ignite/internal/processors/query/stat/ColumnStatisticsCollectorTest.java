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
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.keys.BooleanIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.ByteIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.DateIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.DecimalIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.DoubleIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.FloatIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IntegerIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.NullIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.ShortIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.StringIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.UuidIndexKey;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.table.Column;
import org.junit.Test;

/**
 * Test different scenario with column statistics collection.
 */
public class ColumnStatisticsCollectorTest extends GridCommonAbstractTest {
    /** Types with its comparators for tests.  */
    private static final Set<IndexKey[]> types = new HashSet<>();

    static {
        types.add(new IndexKey[]{new BooleanIndexKey(false), new BooleanIndexKey(true)});
        types.add(new IndexKey[]{new IntegerIndexKey(1), new IntegerIndexKey(2), new IntegerIndexKey(10)});
        types.add(new IndexKey[]{new ShortIndexKey((short)1), new ShortIndexKey((short)3)});
        types.add(new IndexKey[]{new StringIndexKey("1"), new StringIndexKey("9")});
        types.add(new IndexKey[]{new DecimalIndexKey(BigDecimal.ONE), new DecimalIndexKey(BigDecimal.TEN)});
        types.add(new IndexKey[]{
            new DateIndexKey(LocalDate.of(1945, Month.MAY, 9)),
            new DateIndexKey(LocalDate.of(1957, Month.OCTOBER, 4)),
            new DateIndexKey(LocalDate.of(1961, Month.APRIL, 12)),
        });
        types.add(new IndexKey[]{new UuidIndexKey(new UUID(1, 2)), new UuidIndexKey(new UUID(2, 1)),
            new UuidIndexKey(new UUID(2, 2))});
        types.add(new IndexKey[]{new FloatIndexKey(1f), new FloatIndexKey(10f)});
        types.add(new IndexKey[]{new DoubleIndexKey(1.), new DoubleIndexKey(10.)});
        types.add(new IndexKey[]{new ByteIndexKey((byte)1), new ByteIndexKey((byte)2)});
    }

    /**
     * Test different types statistics collection, but without values.
     * Check that statistics collected properly.
     */
    @Test
    public void testZeroAggregation() throws Exception {
        IndexKey[] zeroArr = new IndexKey[0];
        for (IndexKey[] keys : types)
            testAggregation(keys[0].type(), 0, zeroArr);
    }

    /**
     * Test statistics collection with only one null value.
     * Check that statistics collected properly.
     */
    @Test
    public void testSingleNullAggregation() throws Exception {
        for (IndexKey[] keys : types)
            testAggregation(keys[0].type(), 1);
    }

    /**
     * Test statistics collection with multiple null values.
     * Check that statistics collected properly.
     */
    @Test
    public void testMultipleNullsAggregation() throws Exception {
        IndexKey[] zeroArr = new IndexKey[0];
        for (IndexKey[] keys : types)
            testAggregation(keys[0].type(), 1000, zeroArr);
    }

    /**
     * Test statistics collection with multiple values.
     * Check that statistics collected properly.
     */
    @Test
    public void testSingleAggregation() throws Exception {
        for (IndexKey[] keys : types) {
            for (IndexKey key : keys)
                testAggregation(key.type(), 0, key);
        }
    }

    /**
     * Test statistics collection with multiple values.
     * Check that statistics collected properly.
     */
    @Test
    public void testMultipleAggregation() throws Exception {
        for (IndexKey[] keys : types)
            testAggregation(keys[0].type(), 0, keys);
    }

    /**
     * Test statistics collection with multiple values and nulls.
     * Check that statistics collected properly.
     */
    @Test
    public void testMultipleWithNullsAggregation() throws Exception {
        for (IndexKey[] keys : types)
            testAggregation(keys[0].type(), keys.length, keys);
    }

    /**
     * Test aggregation with specified values.
     * Check that statistics collected properly.
     *
     * @param type Value type.
     * @param nulls Nulls count.
     * @param vals Values to aggregate where the first one is the smallest and the last one is the biggest one.
     */
    private static void testAggregation(IndexKeyType type, int nulls, IndexKey... vals) throws Exception {
        Column intCol = new Column("test", type.code());

        ColumnStatisticsCollector collector = new ColumnStatisticsCollector(intCol.getColumnId(), intCol.getName(), type);
        ColumnStatisticsCollector collectorInverted = new ColumnStatisticsCollector(intCol.getColumnId(), intCol.getName(), type);

        for (int i = 0; i < vals.length; i++) {
            collector.add(vals[i]);
            collectorInverted.add(vals[vals.length - 1 - i]);
        }
        for (int i = 0; i < nulls; i++) {
            collector.add(NullIndexKey.INSTANCE);
            collectorInverted.add(NullIndexKey.INSTANCE);
        }

        ColumnStatistics res = collector.finish();
        ColumnStatistics resInverted = collectorInverted.finish();

        testAggregationResult(res, nulls, vals);
        testAggregationResult(resInverted, nulls, vals);
    }

    /**
     * Check column statistics collection results.
     *
     * @param res Column statistics to test.
     * @param nulls Count of null values in statistics.
     * @param vals Values included into statistics where first one is the smallest one and the last one is the biggest.
     */
    private static void testAggregationResult(ColumnStatistics res, int nulls, IndexKey... vals) {
        if (vals.length == 0) {
            assertNull(res.min());
            assertNull(res.max());
        }
        else {
            assertEquals(vals[0].key(), res.min().key());
            assertEquals(vals[vals.length - 1].key(), res.max().key());
        }

        assertEquals(nulls, res.nulls());

        int distinct = (vals.length == 0) ? 0 : new HashSet<>(Arrays.asList(vals)).size();

        assertEquals(distinct, res.distinct());
        assertEquals(vals.length + nulls, res.total());
        assertNotNull(res.raw());
    }
}
