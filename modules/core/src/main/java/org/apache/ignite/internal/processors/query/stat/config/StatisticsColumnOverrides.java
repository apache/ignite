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
package org.apache.ignite.internal.processors.query.stat.config;

import java.io.Serializable;
import java.util.Objects;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Configuration overrides.
 */
public class StatisticsColumnOverrides implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Percent of null values in column. If {@code null} - use calculated value. */
    private final Long nulls;

    /** Number of distinct values in column. If {@code null} - use calculated value. */
    private final Long distinct;

    /** Total number of values in column. If {@code null} - use calculated value. */
    private final Long total;

    /** Average size in bytes, for variable size only. If {@code null} - use calculated value. */
    private final Integer size;

    /**
     * Constructor.
     *
     * @param nulls Number of null values or {@code null} to keep calculated value.
     * @param distinct Number of distinct values or {@code null} to keep calculated value.
     * @param total Total number of values in column or {@code null} to keep calculated value.
     * @param size Average size in bytes or {@code null} to keep calculated value.
     */
    public StatisticsColumnOverrides(Long nulls, Long distinct, Long total, Integer size) {
        this.nulls = nulls;
        this.distinct = distinct;
        this.total = total;
        this.size = size;
    }

    /**
     * Get number of {@code null} values in column.
     *
     * @return Number of null values in column.
     */
    public Long nulls() {
        return nulls;
    }

    /**
     * Get number of distinct values in column.
     *
     * @return Number of distinct values in column.
     */
    public Long distinct() {
        return distinct;
    }

    /**
     * Get total number of values in column.
     *
     * @return Total number of values in column (including {@code null}'s).
     */
    public Long total() {
        return total;
    }

    /**
     * Get average size in bytes.
     *
     * @return Average size in bytes.
     */
    public Integer size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatisticsColumnOverrides that = (StatisticsColumnOverrides) o;
        return Objects.equals(nulls, that.nulls) && Objects.equals(distinct, that.distinct) &&
            Objects.equals(total, that.total) && Objects.equals(size, that.size);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(nulls, distinct, total, size);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StatisticsColumnOverrides.class, this);
    }
}
