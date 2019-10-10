/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import org.h2.util.New;
import org.h2.util.StringUtils;

/**
 * A tool to help an application execute multi-dimensional range queries.
 * The algorithm used is database independent, the only requirement
 * is that the engine supports a range index (for example b-tree).
 */
public class MultiDimension implements Comparator<long[]> {

    private static final MultiDimension INSTANCE = new MultiDimension();

    protected MultiDimension() {
        // don't allow construction by normal code
        // but allow tests
    }

    /**
     * Get the singleton.
     *
     * @return the singleton
     */
    public static MultiDimension getInstance() {
        return INSTANCE;
    }

    /**
     * Normalize a value so that it is between the minimum and maximum for the
     * given number of dimensions.
     *
     * @param dimensions the number of dimensions
     * @param value the value (must be in the range min..max)
     * @param min the minimum value
     * @param max the maximum value (must be larger than min)
     * @return the normalized value in the range 0..getMaxValue(dimensions)
     */
    public int normalize(int dimensions, double value, double min, double max) {
        if (value < min || value > max) {
            throw new IllegalArgumentException(min + "<" + value + "<" + max);
        }
        double x = (value - min) / (max - min);
        return (int) (x * getMaxValue(dimensions));
    }

    /**
     * Get the maximum value for the given dimension count. For two dimensions,
     * each value must contain at most 32 bit, for 3: 21 bit, 4: 16 bit, 5: 12
     * bit, 6: 10 bit, 7: 9 bit, 8: 8 bit.
     *
     * @param dimensions the number of dimensions
     * @return the maximum value
     */
    public int getMaxValue(int dimensions) {
        if (dimensions < 2 || dimensions > 32) {
            throw new IllegalArgumentException("" + dimensions);
        }
        int bitsPerValue = getBitsPerValue(dimensions);
        return (int) ((1L << bitsPerValue) - 1);
    }

    private static int getBitsPerValue(int dimensions) {
        return Math.min(31, 64 / dimensions);
    }

    /**
     * Convert the multi-dimensional value into a one-dimensional (scalar)
     * value. This is done by interleaving the bits of the values. Each values
     * must be between 0 (including) and the maximum value for the given number
     * of dimensions (getMaxValue, excluding). To normalize values to this
     * range, use the normalize function.
     *
     * @param values the multi-dimensional value
     * @return the scalar value
     */
    public long interleave(int... values) {
        int dimensions = values.length;
        long max = getMaxValue(dimensions);
        int bitsPerValue = getBitsPerValue(dimensions);
        long x = 0;
        for (int i = 0; i < dimensions; i++) {
            long k = values[i];
            if (k < 0 || k > max) {
                throw new IllegalArgumentException(0 + "<" + k + "<" + max);
            }
            for (int b = 0; b < bitsPerValue; b++) {
                x |= (k & (1L << b)) << (i + (dimensions - 1) * b);
            }
        }
        return x;
    }

    /**
     * Convert the two-dimensional value into a one-dimensional (scalar) value.
     * This is done by interleaving the bits of the values.
     * Each values must be between 0 (including) and the maximum value
     * for the given number of dimensions (getMaxValue, excluding).
     * To normalize values to this range, use the normalize function.
     *
     * @param x the value of the first dimension, normalized
     * @param y the value of the second dimension, normalized
     * @return the scalar value
     */
    public long interleave(int x, int y) {
        if (x < 0) {
            throw new IllegalArgumentException(0 + "<" + x);
        }
        if (y < 0) {
            throw new IllegalArgumentException(0 + "<" + y);
        }
        long z = 0;
        for (int i = 0; i < 32; i++) {
            z |= (x & (1L << i)) << i;
            z |= (y & (1L << i)) << (i + 1);
        }
        return z;
    }

    /**
     * Gets one of the original multi-dimensional values from a scalar value.
     *
     * @param dimensions the number of dimensions
     * @param scalar the scalar value
     * @param dim the dimension of the returned value (starting from 0)
     * @return the value
     */
    public int deinterleave(int dimensions, long scalar, int dim) {
        int bitsPerValue = getBitsPerValue(dimensions);
        int value = 0;
        for (int i = 0; i < bitsPerValue; i++) {
            value |= (scalar >> (dim + (dimensions - 1) * i)) & (1L << i);
        }
        return value;
    }

    /**
     * Generates an optimized multi-dimensional range query. The query contains
     * parameters. It can only be used with the H2 database.
     *
     * @param table the table name
     * @param columns the list of columns
     * @param scalarColumn the column name of the computed scalar column
     * @return the query
     */
    public String generatePreparedQuery(String table, String scalarColumn,
            String[] columns) {
        StringBuilder buff = new StringBuilder("SELECT D.* FROM ");
        buff.append(StringUtils.quoteIdentifier(table)).
            append(" D, TABLE(_FROM_ BIGINT=?, _TO_ BIGINT=?) WHERE ").
            append(StringUtils.quoteIdentifier(scalarColumn)).
            append(" BETWEEN _FROM_ AND _TO_");
        for (String col : columns) {
            buff.append(" AND ").append(StringUtils.quoteIdentifier(col)).
                append("+1 BETWEEN ?+1 AND ?+1");
        }
        return buff.toString();
    }

    /**
     * Executes a prepared query that was generated using generatePreparedQuery.
     *
     * @param prep the prepared statement
     * @param min the lower values
     * @param max the upper values
     * @return the result set
     */
    public ResultSet getResult(PreparedStatement prep, int[] min, int[] max)
            throws SQLException {
        long[][] ranges = getMortonRanges(min, max);
        int len = ranges.length;
        Long[] from = new Long[len];
        Long[] to = new Long[len];
        for (int i = 0; i < len; i++) {
            from[i] = ranges[i][0];
            to[i] = ranges[i][1];
        }
        prep.setObject(1, from);
        prep.setObject(2, to);
        len = min.length;
        for (int i = 0, idx = 3; i < len; i++) {
            prep.setInt(idx++, min[i]);
            prep.setInt(idx++, max[i]);
        }
        return prep.executeQuery();
    }

    /**
     * Gets a list of ranges to be searched for a multi-dimensional range query
     * where min &lt;= value &lt;= max. In most cases, the ranges will be larger
     * than required in order to combine smaller ranges into one. Usually, about
     * double as many points will be included in the resulting range.
     *
     * @param min the minimum value
     * @param max the maximum value
     * @return the list of ranges (low, high pairs)
     */
    private long[][] getMortonRanges(int[] min, int[] max) {
        int len = min.length;
        if (max.length != len) {
            throw new IllegalArgumentException(len + "=" + max.length);
        }
        for (int i = 0; i < len; i++) {
            if (min[i] > max[i]) {
                int temp = min[i];
                min[i] = max[i];
                max[i] = temp;
            }
        }
        int total = getSize(min, max, len);
        ArrayList<long[]> list = New.arrayList();
        addMortonRanges(list, min, max, len, 0);
        combineEntries(list, total);
        return list.toArray(new long[0][]);
    }

    private static int getSize(int[] min, int[] max, int len) {
        int size = 1;
        for (int i = 0; i < len; i++) {
            int diff = max[i] - min[i];
            size *= diff + 1;
        }
        return size;
    }

    /**
     * Combine entries if the size of the list is too large.
     *
     * @param list list of pairs(low, high)
     * @param total product of the gap lengths
     */
    private void combineEntries(ArrayList<long[]> list, int total) {
        Collections.sort(list, this);
        for (int minGap = 10; minGap < total; minGap += minGap / 2) {
            for (int i = 0; i < list.size() - 1; i++) {
                long[] current = list.get(i);
                long[] next = list.get(i + 1);
                if (current[1] + minGap >= next[0]) {
                    current[1] = next[1];
                    list.remove(i + 1);
                    i--;
                }
            }
            int searched = 0;
            for (long[] range : list) {
                searched += range[1] - range[0] + 1;
            }
            if (searched > 2 * total || list.size() < 100) {
                break;
            }
        }
    }

    @Override
    public int compare(long[] a, long[] b) {
        return a[0] > b[0] ? 1 : -1;
    }

    private void addMortonRanges(ArrayList<long[]> list, int[] min, int[] max,
            int len, int level) {
        if (level > 100) {
            throw new IllegalArgumentException("" + level);
        }
        int largest = 0, largestDiff = 0;
        long size = 1;
        for (int i = 0; i < len; i++) {
            int diff = max[i] - min[i];
            if (diff < 0) {
                throw new IllegalArgumentException(""+ diff);
            }
            size *= diff + 1;
            if (size < 0) {
                throw new IllegalArgumentException("" + size);
            }
            if (diff > largestDiff) {
                largestDiff = diff;
                largest = i;
            }
        }
        long low = interleave(min), high = interleave(max);
        if (high < low) {
            throw new IllegalArgumentException(high + "<" + low);
        }
        long range = high - low + 1;
        if (range == size) {
            long[] item = { low, high };
            list.add(item);
        } else {
            int middle = findMiddle(min[largest], max[largest]);
            int temp = max[largest];
            max[largest] = middle;
            addMortonRanges(list, min, max, len, level + 1);
            max[largest] = temp;
            temp = min[largest];
            min[largest] = middle + 1;
            addMortonRanges(list, min, max, len, level + 1);
            min[largest] = temp;
        }
    }

    private static int roundUp(int x, int blockSizePowerOf2) {
        return (x + blockSizePowerOf2 - 1) & (-blockSizePowerOf2);
    }

    private static int findMiddle(int a, int b) {
        int diff = b - a - 1;
        if (diff == 0) {
            return a;
        }
        if (diff == 1) {
            return a + 1;
        }
        int scale = 0;
        while ((1 << scale) < diff) {
            scale++;
        }
        scale--;
        int m = roundUp(a + 2, 1 << scale) - 1;
        if (m <= a || m >= b) {
            throw new IllegalArgumentException(a + "<" + m + "<" + b);
        }
        return m;
    }

}
