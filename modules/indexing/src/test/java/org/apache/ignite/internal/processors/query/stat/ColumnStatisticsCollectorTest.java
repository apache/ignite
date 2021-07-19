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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.table.Column;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueByte;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueInt;
import org.h2.value.ValueNull;
import org.h2.value.ValueShort;
import org.h2.value.ValueString;
import org.h2.value.ValueUuid;
import org.junit.Test;

/**
 * Test different scenario with column statistics collection.
 */
public class ColumnStatisticsCollectorTest extends GridCommonAbstractTest {
    /** Types with its comparators for tests.  */
    private static final Map<Value[], Comparator<Value>> types = new HashMap<>();

    static {
        types.put(new Value[]{ValueBoolean.get(false), ValueBoolean.get(true)},
            (v1, v2) -> Boolean.compare(v1.getBoolean(), v2.getBoolean()));
        types.put(new Value[]{ValueInt.get(1), ValueInt.get(2), ValueInt.get(10)},
            (v1, v2) -> Integer.compare(v1.getInt(), v2.getInt()));
        types.put(new Value[]{ValueShort.get((short)1), ValueShort.get((short)3)},
            (v1, v2) -> Short.compare(v1.getShort(), v2.getShort()));
        types.put(new Value[]{ValueString.get("1"), ValueString.get("9")},
            (v1, v2) -> v1.getString().compareTo(v2.getString()));
        types.put(new Value[]{ValueDecimal.get(BigDecimal.ONE), ValueDecimal.get(BigDecimal.TEN)},
            (v1, v2) -> v1.getBigDecimal().compareTo(v2.getBigDecimal()));
        types.put(new Value[]{ValueDate.fromMillis(1), ValueDate.fromMillis(10000), ValueDate.fromMillis(9999999)},
            (v1, v2) -> v1.getDate().compareTo(v2.getDate()));
        types.put(new Value[]{ValueUuid.get(1, 2), ValueUuid.get(2, 1), ValueUuid.get(2, 2)},
            (v1, v2) -> new UUID(((ValueUuid)v1).getHigh(), ((ValueUuid)v1).getLow())
                .compareTo(new UUID(((ValueUuid)v2).getHigh(), ((ValueUuid)v2).getLow())));
        types.put(new Value[]{ValueFloat.get(1f), ValueFloat.get(10f)},
            (v1, v2) -> Float.compare(v1.getFloat(), v2.getFloat()));
        types.put(new Value[]{ValueDouble.get(1.), ValueDouble.get(10.)},
            (v1, v2) -> Double.compare(v1.getDouble(), v2.getDouble()));
        types.put(new Value[]{ValueByte.get((byte)1), ValueByte.get((byte)2)},
            (v1, v2) -> Byte.compare(v1.getByte(), v2.getByte()));
    }

    /**
     * Test different types statistics collection, but without values.
     * Check that statistics collected properly.
     */
    @Test
    public void testZeroAggregation() {
        Value[] zeroArr = new Value[0];
        for (Map.Entry<Value[], Comparator<Value>> type : types.entrySet())
            testAggregation(type.getValue(), type.getKey()[0].getType(), 0, zeroArr);
    }

    /**
     * Test statistics collection with only one null value.
     * Check that statistics collected properly.
     */
    @Test
    public void testSingleNullAggregation() {
        for (Map.Entry<Value[], Comparator<Value>> type : types.entrySet())
            testAggregation(type.getValue(), type.getKey()[0].getType(), 1);
    }

    /**
     * Test statistics collection with multiple null values.
     * Check that statistics collected properly.
     */
    @Test
    public void testMultipleNullsAggregation() {
        Value[] zeroArr = new Value[0];
        for (Map.Entry<Value[], Comparator<Value>> type : types.entrySet())
            testAggregation(type.getValue(), type.getKey()[0].getType(), 1000, zeroArr);
    }

    /**
     * Test statistics collection with multiple values.
     * Check that statistics collected properly.
     */
    @Test
    public void testSingleAggregation() {
        for (Map.Entry<Value[], Comparator<Value>> type : types.entrySet()) {
            for (Value v : type.getKey())
                testAggregation(type.getValue(), v.getType(), 0, v);
        }
    }

    /**
     * Test statistics collection with multiple values.
     * Check that statistics collected properly.
     */
    @Test
    public void testMultipleAggregation() {
        for (Map.Entry<Value[], Comparator<Value>> type : types.entrySet()) {
            Value[] vals = type.getKey();
            testAggregation(type.getValue(), vals[0].getType(), 0, vals);
        }
    }

    /**
     * Test statistics collection with multiple values and nulls.
     * Check that statistics collected properly.
     */
    @Test
    public void testMultipleWithNullsAggregation() {
        for (Map.Entry<Value[], Comparator<Value>> type : types.entrySet()) {
            Value[] vals = type.getKey();
            testAggregation(type.getValue(), vals[0].getType(), vals.length, vals);
        }
    }

    /**
     * Test aggregation with specified values.
     * Check that statistics collected properly.
     *
     * @param comp Value comparator.
     * @param type Value type.
     * @param nulls Nulls count.
     * @param vals Values to aggregate where the first one is the smallest and the last one is the biggest one.
     */
    private static void testAggregation(Comparator<Value> comp, int type, int nulls, Value... vals) {
        Column intCol = new Column("test", type);

        ColumnStatisticsCollector collector = new ColumnStatisticsCollector(intCol, comp);
        ColumnStatisticsCollector collectorInverted = new ColumnStatisticsCollector(intCol, comp);

        for (int i = 0; i < vals.length; i++) {
            collector.add(vals[i]);
            collectorInverted.add(vals[vals.length - 1 - i]);
        }
        for (int i = 0; i < nulls; i++) {
            collector.add(ValueNull.INSTANCE);
            collectorInverted.add(ValueNull.INSTANCE);
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
    private static void testAggregationResult(ColumnStatistics res, int nulls, Value... vals) {
        if (vals.length == 0) {
            assertNull(res.min());
            assertNull(res.max());
        }
        else {
            assertEquals(vals[0], res.min());
            assertEquals(vals[vals.length - 1], res.max());
        }

        assertEquals(nulls, res.nulls());

        int distinct = (vals.length == 0) ? 0 : new HashSet<>(Arrays.asList(vals)).size();

        assertEquals(distinct, res.distinct());
        assertEquals(vals.length + nulls, res.total());
        assertNotNull(res.raw());
    }
}
