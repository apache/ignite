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

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;

import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnOverrides;
import org.apache.ignite.internal.processors.query.stat.hll.HLL;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.table.Column;
import org.h2.value.Value;

/**
 * Collector to compute statistic by single column.
 */
public class ColumnStatisticsCollector {
    /** Column. */
    private final Column col;

    /** Hyper Log Log structure */
    private final HLL hll = buildHll();

    /** Minimum value. */
    private Value min = null;

    /** Maximum value. */
    private Value max = null;

    /** Total values in column. */
    private long total = 0;

    /** Total size of all non nulls values (in bytes).*/
    private long size = 0;

    /** Column value comparator. */
    private final Comparator<Value> comp;

    /** Null values counter. */
    private long nullsCnt;

    /** Is column has complex type. */
    private final boolean complexType;

    /** Hasher. */
    private final Hasher hash = new Hasher();

    /** Version. */
    private final long ver;

    /**
     * Constructor.
     *
     * @param col Column to collect statistics by.
     * @param comp Column values comparator.
     */
    public ColumnStatisticsCollector(Column col, Comparator<Value> comp) {
        this(col, comp, 0);
    }

    /**
     * Constructor.
     *
     * @param col Column to collect statistics by.
     * @param comp Column values comparator.
     * @param ver Target statistic version.
     */
    public ColumnStatisticsCollector(Column col, Comparator<Value> comp, long ver) {
        this.col = col;
        this.comp = comp;
        this.ver = ver;

        int colType = col.getType();
        complexType = colType == Value.ARRAY || colType == Value.ENUM || colType == Value.JAVA_OBJECT ||
            colType == Value.RESULT_SET || colType == Value.UNKNOWN;
    }

    /**
     * Try to fix unexpected behaviour of base Value class.
     *
     * @param val Value to convert.
     * @return Byte array.
     */
    private byte[] getBytes(Value val) {
        switch (val.getType()) {
            case Value.STRING:
                String strVal = val.getString();
                return strVal.getBytes(StandardCharsets.UTF_8);
            case Value.BOOLEAN:
                return val.getBoolean() ? new byte[]{1} : new byte[]{0};
            case Value.DECIMAL:
            case Value.DOUBLE:
            case Value.FLOAT:
                return U.join(val.getBigDecimal().unscaledValue().toByteArray(),
                    BigInteger.valueOf(val.getBigDecimal().scale()).toByteArray());
            case Value.TIME:
                return BigInteger.valueOf(val.getTime().getTime()).toByteArray();
            case Value.DATE:
                return BigInteger.valueOf(val.getDate().getTime()).toByteArray();
            case Value.TIMESTAMP:
                return BigInteger.valueOf(val.getTimestamp().getTime()).toByteArray();
            default:
                return val.getBytes();
        }
    }

    /**
     * Add value to statistics.
     *
     * @param val Value to add to statistics.
     */
    public void add(Value val) {
        total++;

        if (isNullValue(val)) {
            nullsCnt++;

            return;
        }

        byte[] bytes = getBytes(val);
        size += bytes.length;

        hll.addRaw(hash.fastHash(bytes));

        if (!complexType) {
            if (null == min || comp.compare(val, min) < 0)
                min = val;

            if (null == max || comp.compare(val, max) > 0)
                max = val;
        }
    }

    /**
     * Get total column statistics.
     *
     * @return Aggregated column statistics.
     */
    public ColumnStatistics finish() {

        int averageSize = averageSize(size, total, nullsCnt);

        return new ColumnStatistics(min, max, nullsCnt, hll.cardinality(), total, averageSize, hll.toBytes(), ver,
            U.currentTimeMillis());
    }

    /**
     * Calculate average record size in bytes.
     *
     * @param size Total size of all records.
     * @param total Total number of all records.
     * @param nullsCnt Number of nulls record.
     * @return Average size of not null record in byte.
     */
    private static int averageSize(long size, long total, long nullsCnt) {
        long averageSizeLong = (total - nullsCnt > 0) ? (size / (total - nullsCnt)) : 0;
        return (averageSizeLong > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) averageSizeLong;
    }

    /**
     * @return get column.
     */
    public Column col() {
        return col;
    }

    /**
     * Aggregate specified (partition or local) column statistics into (local or global) single one.
     *
     * @param comp Value comparator.
     * @param partStats Column statistics by partitions.
     * @param overrides Overrides or {@code null} to keep calculated values.
     * @return Column statistics for all partitions.
     */
    public static ColumnStatistics aggregate(
        Comparator<Value> comp,
        List<ColumnStatistics> partStats,
        StatisticsColumnOverrides overrides
    ) {
        assert !F.isEmpty(partStats);

        Long overrideDistinct = (overrides == null) ? null : overrides.distinct();
        HLL hll = buildHll();

        Value min = null;
        Value max = null;

        // Total number of nulls
        long nullsCnt = 0;

        // Total values (null and not null) counter)
        long total = 0;

        // Total size in bytes
        long totalSize = 0;

        ColumnStatistics firstStat = F.first(partStats);
        long ver = firstStat.version();
        long createdAt = firstStat.createdAt();

        for (ColumnStatistics partStat : partStats) {
            assert ver == partStat.version() : "Aggregate statistics with different version [stats=" + partStats + ']';

            if (overrideDistinct == null) {
                HLL partHll = HLL.fromBytes(partStat.raw());
                hll.union(partHll);
            }

            total += partStat.total();
            nullsCnt += partStat.nulls();
            totalSize += (long)partStat.size() * (partStat.total() - partStat.nulls());

            if (min == null || (partStat.min() != null && comp.compare(partStat.min(), min) < 0))
                min = partStat.min();

            if (max == null || (partStat.max() != null && comp.compare(partStat.max(), max) > 0))
                max = partStat.max();

            if (createdAt < partStat.createdAt())
                createdAt = partStat.createdAt();
        }

        Integer overrideSize = (overrides == null) ? null : overrides.size();
        int averageSize = (overrideSize == null) ? averageSize(totalSize, total, nullsCnt) : overrideSize;

        long distinct = (overrideDistinct == null) ? hll.cardinality() : overrideDistinct;

        Long overrideNulls = (overrides == null) ? null : overrides.nulls();
        long nulls = (overrideNulls == null) ? nullsCnt : overrideNulls;

        Long overrideTotal = (overrides == null) ? null : overrides.total();
        total = (overrideTotal == null) ? total : overrideTotal;

        return new ColumnStatistics(min, max, nulls, distinct, total, averageSize, hll.toBytes(), ver, createdAt);
    }

    /**
     * Get HLL with default params.
     *
     * @return Empty hll structure.
     */
    private static HLL buildHll() {
        return new HLL(13, 5);
    }

    /**
     * Test if specified value is null.
     *
     * @param v Value to test.
     * @return {@code true} if value is null, {@code false} - otherwise.
     */
    public static boolean isNullValue(Value v) {
        return v == null || v.getType() == Value.NULL;
    }
}
