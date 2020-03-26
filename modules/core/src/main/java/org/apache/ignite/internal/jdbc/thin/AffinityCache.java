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

package org.apache.ignite.internal.jdbc.thin;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.GridBoundedLinkedHashMap;

/**
 * Affinity Cache.
 */
public final class AffinityCache {
    /** Affinity topology version. */
    private final AffinityTopologyVersion ver;

    /** Cache partitions distribution. */
    private final GridBoundedLinkedHashMap<Integer, UUID[]> cachePartitionsDistribution;

    /** Sql cache. */
    private final GridBoundedLinkedHashMap<QualifiedSQLQuery, JdbcThinPartitionResultDescriptor> sqlCache;

    /**
     * Constructor.
     *
     * @param ver Affinity topology version.
     */
    public AffinityCache(AffinityTopologyVersion ver, int partitionAwarenessPartDistributionsCacheSize,
        int partitionAwarenessSQLCacheSize) {
        this.ver = ver;

        cachePartitionsDistribution = new GridBoundedLinkedHashMap<>(partitionAwarenessPartDistributionsCacheSize);

        sqlCache = new GridBoundedLinkedHashMap<>(partitionAwarenessSQLCacheSize);
    }

    /**
     * @return Version.
     */
    public AffinityTopologyVersion version() {
        return ver;
    }

    /**
     * Adds cache distribution related to the cache with specified cache id.
     *
     * @param cacheId Cache Id.
     * @param distribution Cache partitions distribution, where partition id is an array index.
     */
    void addCacheDistribution(Integer cacheId, UUID[] distribution) {
        for (Map.Entry<Integer, UUID[]> entry : cachePartitionsDistribution.entrySet()) {
            if (Arrays.equals(entry.getValue(), distribution)) {
                // put link to already existing distribution instead of creating new one.
                cachePartitionsDistribution.put(cacheId, entry.getValue());

                return;
            }
        }

        cachePartitionsDistribution.put(cacheId, distribution);
    }

    /**
     * Adds sql query with corresponding partition result descriptor.
     *
     * @param sql Qualified sql query.
     * @param partRes Partition result descriptor.
     */
    void addSqlQuery(QualifiedSQLQuery sql, JdbcThinPartitionResultDescriptor partRes) {
        sqlCache.put(sql, partRes == null ? JdbcThinPartitionResultDescriptor.EMPTY_DESCRIPTOR : partRes);
    }

    /**
     * Retrieves partition result descriptor related to corresponding sql query.
     *
     * @param sqlQry Qualified sql query.
     * @return Partition result descriptor or null.
     */
    public JdbcThinPartitionResultDescriptor partitionResult(QualifiedSQLQuery sqlQry) {
        return sqlCache.get(sqlQry);
    }

    /**
     * @param cacheId Cache Id.
     * @return Cache partition distribution for given cache Id or null.
     */
    public UUID[] cacheDistribution(int cacheId) {
        return cachePartitionsDistribution.get(cacheId);
    }
}
