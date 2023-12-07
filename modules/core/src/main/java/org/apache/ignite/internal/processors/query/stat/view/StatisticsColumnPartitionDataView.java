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

package org.apache.ignite.internal.processors.query.stat.view;

import org.apache.ignite.internal.managers.systemview.walker.Filtrable;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.query.stat.ObjectPartitionStatisticsImpl;
import org.apache.ignite.internal.processors.query.stat.StatisticsKey;

/**
 * Statistics partition data view.
 */
public class StatisticsColumnPartitionDataView {
    /** Statistics key */
    private final StatisticsKey key;

    /** Object statistics. */
    private final ObjectPartitionStatisticsImpl statistics;

    /** Column name. */
    private final String column;

    /**
     * Constructor.
     *
     * @param key Statistics key.
     * @param column Column name.
     * @param statistics Object partition statistics.
     */
    public StatisticsColumnPartitionDataView(StatisticsKey key, String column, ObjectPartitionStatisticsImpl statistics) {
        this.key = key;
        this.column = column;
        this.statistics = statistics;
    }

    /**
     * @return Schema name.
     */
    @Order
    @Filtrable
    public String schema() {
        return key.schema();
    }

    /**
     * @return Object type.
     */
    @Order(1)
    @Filtrable
    public String type() {
        return StatisticsColumnConfigurationView.TABLE_TYPE;
    }

    /**
     * @return Object name.
     */
    @Order(2)
    @Filtrable
    public String name() {
        return key.obj();
    }

    /**
     * @return Column name.
     */
    @Order(3)
    @Filtrable
    public String column() {
        return column;
    }

    /**
     * @return Partition id.
     */
    @Order(4)
    @Filtrable
    public int partition() {
        return statistics.partId();
    }

    /**
     * @return Object's row in partition.
     */
    @Order(5)
    public long rowsCount() {
        return statistics.rowCount();
    }

    /**
     * @return Partitions update counter.
     */
    @Order(6)
    public long updateCounter() {
        return statistics.updCnt();
    }

    /**
     * @return Number of distinct values in column.
     */
    @Order(7)
    public long distinct() {
        return statistics.columnStatistics(column).distinct();
    }

    /**
     * @return Number of nulls values.
     */
    @Order(8)
    public long nulls() {
        return statistics.columnStatistics(column).nulls();
    }

    /**
     * @return Total number of values in column in partition.
     */
    @Order(9)
    public long total() {
        return statistics.columnStatistics(column).total();
    }

    /**
     * @return Average size in bytes, for variable size only.
     */
    @Order(10)
    public int size() {
        return statistics.columnStatistics(column).size();
    }

    /**
     * @return Statistic's version.
     */
    @Order(11)
    public long version() {
        return statistics.columnStatistics(column).version();
    }

    /**
     * @return Last update time, milliseconds.
     */
    @Order(12)
    public long lastUpdateTime() {
        return statistics.columnStatistics(column).createdAt();
    }
}
