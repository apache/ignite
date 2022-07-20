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

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnOverrides;
import org.apache.ignite.internal.processors.query.stat.hll.HLL;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Collector to compute statistic by single column.
 */
public class ColumnStatisticsCollector {
    /** Column name. */
    private final String colName;

    /** Column id. */
    private final int colId;

    /** Hyper Log Log structure */
    private final HLL hll = buildHll();

    /** Minimum value. */
    private IndexKey min;

    /** Maximum value. */
    private IndexKey max;

    /** Total values in column. */
    private long total;

    /** Total size of all non nulls values (in bytes).*/
    private long size;

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
     */
    public ColumnStatisticsCollector(int colId, String colName, IndexKeyType colType) {
        this(colId, colName, colType, 0);
    }

    /**
     * Constructor.
     *
     * @param ver Target statistic version.
     */
    public ColumnStatisticsCollector(int colId, String colName, IndexKeyType colType, long ver) {
        this.colId = colId;
        this.colName = colName;
        this.ver = ver;

        complexType = colType == IndexKeyType.ARRAY || colType == IndexKeyType.ENUM ||
                colType == IndexKeyType.JAVA_OBJECT || colType == IndexKeyType.RESULT_SET ||
                colType == IndexKeyType.UNKNOWN;
    }

    /**
     * Add value to statistics.
     *
     * @param val Value to add to statistics.
     */
    public void add(IndexKey val) throws IgniteCheckedException {
        total++;

        if (isNullValue(val)) {
            nullsCnt++;

            return;
        }

        byte[] bytes = val.bytes();
        size += bytes.length;

        hll.addRaw(hash.fastHash(bytes));

        if (!complexType) {
            if (null == min || val.compare(min) < 0)
                min = val;

            if (null == max || val.compare(max) > 0)
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
        return (averageSizeLong > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int)averageSizeLong;
    }

    /**
     * @return Column id.
     */
    public int columnId() {
        return colId;
    }

    /**
     * @return Column name.
     */
    public String columnName() {
        return colName;
    }

    /**
     * Aggregate specified (partition or local) column statistics into (local or global) single one.
     *
     * @param partStats Column statistics by partitions.
     * @param overrides Overrides or {@code null} to keep calculated values.
     * @return Column statistics for all partitions.
     */
    public static ColumnStatistics aggregate(
        List<ColumnStatistics> partStats,
        StatisticsColumnOverrides overrides
    ) {
        assert !F.isEmpty(partStats);

        Long overrideDistinct = (overrides == null) ? null : overrides.distinct();
        HLL hll = buildHll();

        IndexKey min = null;
        IndexKey max = null;

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

            try {
                if (min == null || (partStat.min() != null && partStat.min().compare(min) < 0))
                    min = partStat.min();

                if (max == null || (partStat.max() != null && partStat.max().compare(max) > 0))
                    max = partStat.max();

                if (createdAt < partStat.createdAt())
                    createdAt = partStat.createdAt();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
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
    public static boolean isNullValue(IndexKey v) {
        return v == null || v.type() == IndexKeyType.NULL;
    }
}
