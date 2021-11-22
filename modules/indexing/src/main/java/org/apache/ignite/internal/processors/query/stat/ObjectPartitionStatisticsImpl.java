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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Statistic for some partition of data object.
 */
public class ObjectPartitionStatisticsImpl extends ObjectStatisticsImpl {
    /** Partition id. */
    private final int partId;

    /** Partition update counter at the moment when statistics collected. */
    private final long updCnt;

    /**
     * Constructor.
     *
     * @param partId Partition id.
     * @param rowsCnt Total count of rows in partition.
     * @param updCnt Update counter of partition.
     * @param colNameToStat Column key to column statistics map.
     */
    public ObjectPartitionStatisticsImpl(
        int partId,
        long rowsCnt,
        long updCnt,
        Map<String, ColumnStatistics> colNameToStat
    ) {
        super(rowsCnt, colNameToStat);

        this.partId = partId;
        this.updCnt = updCnt;
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

    /** {@inheritDoc} */
    @Override public ObjectPartitionStatisticsImpl clone() {
        return new ObjectPartitionStatisticsImpl(partId, rowCount(), updCnt, new HashMap<>(columnsStatistics()));
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        if (!super.equals(o))
            return false;

        ObjectPartitionStatisticsImpl that = (ObjectPartitionStatisticsImpl) o;

        return partId == that.partId &&
            updCnt == that.updCnt;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), partId, updCnt);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ObjectPartitionStatisticsImpl.class, this);
    }
}
