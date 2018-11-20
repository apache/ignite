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

package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Holds the partition calculation info extracted from a query.
 * The query may have several such items associated with it.
 *
 * The query may contain expressions containing key or affinity key.
 * Such expressions can be used as hints to derive small isolated set
 * of partitions the query needs to run on.
 *
 * In case expression contains constant (e.g. _key = 100), the partition
 * can be calculated right away and saved into cache along with the query.
 *
 * In case expression has a parameter (e.g. _key = ?), the effective
 * partition varies with each run of the query. Hence, instead of partition,
 * one must store the info required to calculate partition.
 *
 * The given class holds the required info, so that effective partition
 * can be calculated during query parameter binding.
 */
public class CacheQueryPartitionInfo {
    /** */
    private final int partId;

    /** */
    private final String cacheName;

    /** */
    private final String tableName;

    /** */
    private final int dataType;

    /** */
    private final int paramIdx;

    /**
     * @param partId Partition id, or -1 if parameter binding required.
     * @param cacheName Cache name required for partition calculation.
     * @param tableName Table name required for proper type conversion.
     * @param dataType Required data type id for the query parameter.
     * @param paramIdx Query parameter index required for partition calculation.
     */
    public CacheQueryPartitionInfo(int partId, String cacheName, String tableName, int dataType, int paramIdx) {
        // In case partition is not known, both cacheName and tableName must be provided.
        assert (partId >= 0) ^ ((cacheName != null) && (tableName != null));

        this.partId = partId;
        this.cacheName = cacheName;
        this.tableName = tableName;
        this.dataType = dataType;
        this.paramIdx = paramIdx;
    }

    /**
     * @return Partition id, or -1 if parameter binding is required to calculate partition.
     */
    public int partition() {
        return partId;
    }

    /**
     * @return Cache name required for partition calculation.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tableName;
    }

    /**
     * @return Required data type for the query parameter.
     */
    public int dataType() {
        return dataType;
    }

    /**
     * @return Query parameter index required for partition calculation.
     */
    public int paramIdx() {
        return paramIdx;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return partId ^ dataType ^ paramIdx ^
            (cacheName == null ? 0 : cacheName.hashCode()) ^
            (tableName == null ? 0 : tableName.hashCode());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("SimplifiableIfStatement")
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (!(obj instanceof CacheQueryPartitionInfo))
            return false;

        CacheQueryPartitionInfo other = (CacheQueryPartitionInfo)obj;

        if (partId >= 0)
            return partId == other.partId;

        if (other.cacheName == null || other.tableName == null)
            return false;

        return other.cacheName.equals(cacheName) &&
            other.tableName.equals(tableName) &&
            other.dataType == dataType &&
            other.paramIdx == paramIdx;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheQueryPartitionInfo.class, this);
    }
}
