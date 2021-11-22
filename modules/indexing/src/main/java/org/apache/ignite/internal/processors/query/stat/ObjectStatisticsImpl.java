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

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * All statistics by some object (table or index).
 */
public class ObjectStatisticsImpl implements Cloneable, ObjectStatistics {
    /** Total number of rows in object. */
    private final long rowsCnt;

    /** Map columnKey to its statistic. */
    @GridToStringInclude
    private final Map<String, ColumnStatistics> colNameToStat;

    /**
     * Constructor.
     *
     * @param rowsCnt Total rows count.
     * @param colNameToStat Column names to statistics map.
     */
    public ObjectStatisticsImpl(
        long rowsCnt,
        Map<String, ColumnStatistics> colNameToStat
    ) {
        assert rowsCnt >= 0 : "rowsCnt >= 0";

        assert colNameToStat != null : "colNameToStat != null";

        this.rowsCnt = rowsCnt;
        this.colNameToStat = colNameToStat;
    }

    /**
     * @return Object rows count.
     */
    public long rowCount() {
        return rowsCnt;
    }

    /**
     * Get column statistics.
     *
     * @param colName Column name.
     * @return Column statistics or {@code null} if there are no statistics for specified column.
     */
    public ColumnStatistics columnStatistics(String colName) {
        return colNameToStat.get(colName);
    }

    /**
     * @return Column statistics map.
     */
    public Map<String, ColumnStatistics> columnsStatistics() {
        return colNameToStat;
    }

    /** {@inheritDoc} */
    @Override public ObjectStatisticsImpl clone() {
        return new ObjectStatisticsImpl(rowsCnt, new HashMap<>(colNameToStat));
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ObjectStatisticsImpl that = (ObjectStatisticsImpl) o;

        return rowsCnt == that.rowsCnt
            && Objects.equals(colNameToStat, that.colNameToStat);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(rowsCnt, colNameToStat);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ObjectStatisticsImpl.class, this);
    }
}
