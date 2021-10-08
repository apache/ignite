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

import java.util.Arrays;
import java.util.Objects;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.h2.value.Value;

/**
 * Values statistic in particular column.
 */
public class ColumnStatistics {
    /** Minimum value in column or {@code null} if there are no non null values in the column. */
    private final Value min;

    /** Maximum value in column or {@code null} if there are no non null values in the column. */
    private final Value max;

    /** Number of null values in column. */
    private final long nulls;

    /** Number of distinct values in column. */
    private final long distinct;

    /** Total number of values in column. */
    private final long total;

    /** Average size in bytes, for variable size only. */
    private final int size;

    /** Raw data. */
    private final byte[] raw;

    /** Version. */
    private final long ver;

    /** Created at time, milliseconds. */
    private final long createdAt;

    /**
     * Constructor.
     *
     * @param min Min value in column or {@code null}.
     * @param max Max value in column or {@code null}.
     * @param nulls Number of null values in column.
     * @param distinct Number of distinct values in column.
     * @param total Total number of values in column.
     * @param size Average size in bytes, for variable size only.
     * @param raw Raw data to aggregate statistics.
     * @param ver Statistics version.
     * @param createdAt Created at time, milliseconds.
     */
    public ColumnStatistics(
        Value min,
        Value max,
        long nulls,
        long distinct,
        long total,
        int size,
        byte[] raw,
        long ver,
        long createdAt
    ) {
        this.min = min;
        this.max = max;
        this.nulls = nulls;
        this.distinct = distinct;
        this.total = total;
        this.size = size;
        this.raw = raw;
        this.ver = ver;
        this.createdAt = createdAt;
    }

    /**
     * @return Min value in column.
     */
    public Value min() {
        return min;
    }

    /**
     * @return Max value in column.
     */
    public Value max() {
        return max;
    }

    /**
     * @return Number of null values in column.
     */
    public long nulls() {
        return nulls;
    }

    /**
     * @return Number of null values in column.
     */
    public long distinct() {
        return distinct;
    }

    /**
     * @return Total number of values in column.
     */
    public long total() {
        return total;
    }

    /**
     * @return Average size in bytes, for variable size only.
     */
    public int size() {
        return size;
    }

    /**
     * @return Raw value needed to aggregate statistics.
     */
    public byte[] raw() {
        return raw;
    }

    /**
     * @return Statistic's version.
     */
    public long version() {
        return ver;
    }

    /**
     * @return Created at time, milliseconds
     */
    public long createdAt() {
        return createdAt;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnStatistics that = (ColumnStatistics) o;
        return nulls == that.nulls &&
            distinct == that.distinct &&
            total == that.total &&
            size == that.size &&
            ver == that.ver &&
            createdAt == that.createdAt &&
            Objects.equals(min, that.min) &&
            Objects.equals(max, that.max) &&
            Arrays.equals(raw, that.raw);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = Objects.hash(min, max, nulls, distinct, total, size, ver, createdAt);
        result = 31 * result + Arrays.hashCode(raw);
        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ColumnStatistics.class, this);
    }
}
