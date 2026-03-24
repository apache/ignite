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
import java.util.Map;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.query.stat.StatisticsType;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Statistics for some object (index or table) in database.
 */
public class StatisticsObjectData implements Message, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 185;

    /** Statistics key. */
    @Order(0)
    StatisticsKeyMessage key;

    /** Total row count in current object. */
    @Order(1)
    long rowsCnt;

    /** Type of statistics. */
    @Order(2)
    StatisticsType type;

    /** Partition id if statistics was collected by partition. */
    @Order(3)
    int partId;

    /** Update counter if statistics was collected by partition. */
    @Order(4)
    long updCnt;

    /** Columns key to statistic map. */
    @Order(5)
    Map<String, StatisticsColumnData> data;

    /**
     * Constructor.
     *
     * @param key Statistics key.
     * @param rowsCnt Total row count.
     * @param type Statistics type.
     * @param partId Partition id.
     * @param updCnt Partition update counter.
     * @param data Map of statistics column data.
     */
    public StatisticsObjectData(
        StatisticsKeyMessage key,
        long rowsCnt,
        StatisticsType type,
        int partId,
        long updCnt,
        Map<String, StatisticsColumnData> data
    ) {
        this.key = key;
        this.rowsCnt = rowsCnt;
        this.type = type;
        this.partId = partId;
        this.updCnt = updCnt;
        this.data = data;
    }

    /**
     * @return Statistics key.
     */
    public StatisticsKeyMessage key() {
        return key;
    }

    /**
     * @return Total rows count.
     */
    public long rowsCnt() {
        return rowsCnt;
    }

    /**
     * @return Statistics type.
     */
    public StatisticsType type() {
        return type;
    }

    /**
     * @return Partition id.
     */
    public int partId() {
        return partId;
    }

    /**
     * @return Partition update counter.
     */
    public long updCnt() {
        return updCnt;
    }

    /**
     * @return Statistics column data.
     */
    public Map<String, StatisticsColumnData> data() {
        return data;
    }

    /**
     * Default constructor.
     */
    public StatisticsObjectData() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

}
