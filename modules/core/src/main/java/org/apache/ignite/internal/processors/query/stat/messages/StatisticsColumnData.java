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

package org.apache.ignite.internal.processors.query.stat.messages;

import java.io.Serializable;
import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Statistics by column (or by set of columns, if they collected together)
 */
public class StatisticsColumnData implements Message, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 186;

    /** Min value in column. */
    @Order(0)
    StatisticsDecimalMessage min;

    /** Max value in column. */
    @Order(1)
    StatisticsDecimalMessage max;

    /** Number of null values in column. */
    @Order(2)
    long nulls;

    /** Number of distinct values in column (except nulls). */
    @Order(3)
    long distinct;

    /** Total vals in column. */
    @Order(4)
    long total;

    /** Average size, for variable size values (in bytes). */
    @Order(5)
    int size;

    /** Raw data. */
    @Order(6)
    byte[] rawData;

    /** Version. */
    @Order(7)
    long ver;

    /** Created at time, milliseconds. */
    @Order(8)
    long createdAt;

    /**
     * Default constructor.
     */
    public StatisticsColumnData() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param min Min value in column.
     * @param max Max value in column.
     * @param nulls Number of null values in column.
     * @param distinct Total distinct values in column.
     * @param total Total values in column.
     * @param size Average size, for variable size types (in bytes).
     * @param rawData Raw data to make statistics aggregate.
     * @param ver Statistics version.
     * @param createdAt Created at time, milliseconds.
     */
    public StatisticsColumnData(
        StatisticsDecimalMessage min,
        StatisticsDecimalMessage max,
        long nulls,
        long distinct,
        long total,
        int size,
        byte[] rawData,
        long ver,
        long createdAt
    ) {
        this.min = min;
        this.max = max;
        this.nulls = nulls;
        this.distinct = distinct;
        this.total = total;
        this.size = size;
        this.rawData = rawData;
        this.ver = ver;
        this.createdAt = createdAt;
    }

    /**
     * @return Min value in column.
     */
    public StatisticsDecimalMessage min() {
        return min;
    }

    /**
     * @return Max value in column.
     */
    public StatisticsDecimalMessage max() {
        return max;
    }

    /**
     * @return Number of null values in column.
     */
    public long nulls() {
        return nulls;
    }

    /**
     * @return Total distinct values in column.
     */
    public long distinct() {
        return distinct;
    }

    /**
     * @return Total values in column.
     */
    public long total() {
        return total;
    }

    /**
     * @return Average size, for variable size types (in bytes).
     */
    public int size() {
        return size;
    }

    /**
     * @return Raw data.
     */
    public byte[] rawData() {
        return rawData;
    }

    /**
     * @return Raw data.
     */
    public long version() {
        return ver;
    }

    /**
     * @return Created at time, milliseconds.
     */
    public long createdAt() {
        return createdAt;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

}
