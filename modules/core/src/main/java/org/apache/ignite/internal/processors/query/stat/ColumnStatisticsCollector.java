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
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.cache.query.index.IndexProcessor;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnOverrides;
import org.apache.ignite.internal.processors.query.stat.hll.HLL;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils.convertToSqlDate;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils.convertToSqlTime;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils.convertToTimestamp;
import static org.apache.ignite.internal.processors.query.stat.StatisticsUtils.toDecimal;

/**
 * Collector to compute statistic by single column.
 */
public class ColumnStatisticsCollector {
    /** */
    private static final Set<Class<?>> comparableCls = new HashSet<>(Arrays.<Class<?>>asList(
        Boolean.class,
        Byte.class,
        Short.class,
        Integer.class,
        Long.class,
        BigDecimal.class,
        Double.class,
        Float.class,
        Time.class,
        Timestamp.class,
        Date.class,
        java.sql.Date.class,
        LocalTime.class,
        LocalDate.class,
        LocalDateTime.class,
        UUID.class
    ));

    /** Column name. */
    private final String colName;

    /** Column id. */
    private final int colId;

    /** Hyper Log Log structure */
    private final HLL hll = buildHll();

    /** Minimum value. */
    private BigDecimal min;

    /** Maximum value. */
    private BigDecimal max;

    /** Total values in column. */
    private long total;

    /** Total size of all non nulls values (in bytes).*/
    private long size;

    /** Null values counter. */
    private long nullsCnt;

    /** Is column has complex type. */
    private final boolean isComparable;

    /** Hasher. */
    private final Hasher hash = new Hasher();

    /** Version. */
    private final long ver;

    /** Column type. */
    private final Class<?> colType;

    /**
     * Constructor.
     */
    public ColumnStatisticsCollector(int colId, String colName, Class<?> colType) {
        this(colId, colName, colType, 0);
    }

    /**
     * Constructor.
     *
     * @param ver Target statistic version.
     */
    public ColumnStatisticsCollector(int colId, String colName, Class<?> colType, long ver) {
        this.colId = colId;
        this.colName = colName;
        this.ver = ver;
        this.colType = colType;

        isComparable = colType != null && comparableCls.contains(colType);
    }

    /**
     * Add value to statistics.
     *
     * @param val Value to add to statistics.
     */
    public void add(Object val) throws IgniteCheckedException {
        total++;

        if (val == null) {
            nullsCnt++;

            return;
        }

        addToHll(val);

        if (isComparable) {
            BigDecimal decVal = toDecimal(val);

            if (null == min || min.compareTo(decVal) > 0)
                min = decVal;

            if (null == max || max.compareTo(decVal) < 0)
                max = decVal;
        }
    }

    /**
     * Get total column statistics.
     *
     * @return Aggregated column statistics.
     */
    public ColumnStatistics finish() {
        int averageSize = averageSize(size, total, nullsCnt);

        return new ColumnStatistics(toDecimal(min), toDecimal(max), nullsCnt, hll.cardinality(), total, averageSize,
            hll.toBytes(), ver, U.currentTimeMillis());
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
     * @return Column type.
     */
    public Class<?> columnType() {
        return colType;
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

        BigDecimal min = null;
        BigDecimal max = null;

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

            if (min == null || (partStat.min() != null && partStat.min().compareTo(min) < 0))
                min = partStat.min();

            if (max == null || (partStat.max() != null && partStat.max().compareTo(max) > 0))
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

    /** */
    private void addToHll(Object obj) {
        assert obj != null;

        Class<?> cls = U.box(obj.getClass());

        byte[] buf;
        if (Boolean.class.isAssignableFrom(cls))
            buf = new byte[]{(Boolean)obj ? (byte)1 : (byte)0};
        else if (Byte.class.isAssignableFrom(cls))
            buf = new byte[] {(Byte)obj};
        else if (Short.class.isAssignableFrom(cls))
            buf = U.shortToBytes((Short)obj);
        else if (Integer.class.isAssignableFrom(cls))
            buf = U.intToBytes((Integer)obj);
        else if (Long.class.isAssignableFrom(cls))
            buf = U.longToBytes((Long)obj);
        else if (Float.class.isAssignableFrom(cls))
            buf = U.intToBytes(Float.floatToIntBits((Float)obj));
        else if (Double.class.isAssignableFrom(cls))
            buf = U.longToBytes(Double.doubleToLongBits((Double)obj));
        else if (BigDecimal.class.isAssignableFrom(cls)) {
            BigInteger unscaledVal = ((BigDecimal)obj).unscaledValue();
            int scale = ((BigDecimal)obj).scale();
            buf = U.join(unscaledVal.toByteArray(), U.intToBytes(scale));
        }
        else if (UUID.class.isAssignableFrom(cls))
            buf = U.uuidToBytes((UUID)obj);
        else if (LocalDate.class.isAssignableFrom(cls))
            buf = U.longToBytes(convertToSqlDate((LocalDate)obj).getTime());
        else if (LocalTime.class.isAssignableFrom(cls))
            buf = U.longToBytes(convertToSqlTime((LocalTime)obj).getTime());
        else if (LocalDateTime.class.isAssignableFrom(cls))
            buf = timestampToBytes(convertToTimestamp((LocalDateTime)obj));
        else if (Timestamp.class.isAssignableFrom(cls))
            buf = timestampToBytes((Timestamp)obj);
        else if (java.util.Date.class.isAssignableFrom(cls))
            buf = U.longToBytes(((Date)obj).getTime());
        else if (cls.isAssignableFrom(byte[].class))
            buf = (byte[])obj;
        else if (cls.isAssignableFrom(String.class))
            buf = ((String)obj).getBytes(StandardCharsets.UTF_8);
        else if (obj instanceof BinaryObjectImpl)
            buf = ((BinaryObjectImpl)obj).bytes();
        else {
            try {
                buf = IndexProcessor.serializer.serialize(obj);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        size += buf.length;
        hll.addRaw(hash.fastHash(buf));
    }

    /** */
    private static byte[] timestampToBytes(java.sql.Timestamp ts) {
        byte[] buf = new byte[12];

        U.longToBytes(ts.getTime(), buf, 0);
        U.intToBytes(ts.getNanos(), buf, 8);

        return buf;
    }
}
