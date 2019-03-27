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

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.GridBoundedLinkedHashMap;

/**
 * Affinity Cache.
 */
public final class AffinityCache {
    /** Partition distributions cache limit. */
    public static final int DISTRIBUTIONS_CACHE_LIMIT = 1000;

    /** SQL cache limit. */
    public static final int SQL_CACHE_LIMIT = 100_000;

    /** Affinity topology version. */
    private final AffinityTopologyVersion ver;

    /** Cache partitions distribution. */
    private final GridBoundedLinkedHashMap<Integer, Map<Integer, UUID>> cachePartitionsDistribution;

    /** Sql cache. */
    private final GridBoundedLinkedHashMap<String, JdbcThinPartitionResultDescriptor> sqlCache;

    /**
     * Constructor.
     *
     * @param ver Affinity topology version.
     */
    public AffinityCache(AffinityTopologyVersion ver) {
        this.ver = ver;

        cachePartitionsDistribution = new GridBoundedLinkedHashMap<>(DISTRIBUTIONS_CACHE_LIMIT);

        sqlCache = new GridBoundedLinkedHashMap<>(SQL_CACHE_LIMIT);
    }

    /**
     * @return Version.
     */
    public AffinityTopologyVersion version() {
        return ver;
    }

    /**
     * Adds cache distribution related to the cache with specified cache id.
     * @param cacheId Cache Id.
     * @param distribution Cache partitions distribution.
     */
    void addCacheDistribution(Integer cacheId, Map<Integer, UUID> distribution) {
        cachePartitionsDistribution.put(cacheId, distribution);
    }

    /**
     * Adds sql query with corresponding partion result descriptor.
     * @param sql Plain sql query.
     * @param partRes Partition result descriptor.
     */
    void addSqlQuery(String sql, JdbcThinPartitionResultDescriptor partRes) {
        sqlCache.put(sql, partRes);
    }

    /**
     * Retrieves partition result descriptor related to corresponding sql query.
     * @param sqlQry Plain sql query.
     * @return Partition result descriptor or null.
     */
    JdbcThinPartitionResultDescriptor partitionResult(String sqlQry) {
        return sqlCache.get(sqlQry);
    }

    /**
     * @param sqlQry Plain sql
     * @return Returns true if this cache contains a mapping for the specified sql query.
     */
    boolean containsPartitionResult(String sqlQry) {
        return sqlCache.containsKey(sqlQry);
    }

    /**
     * @param cacheId Cache Id.
     * @return Cache partitoins distribution for given cache Id or null.
     */
    Map<Integer, UUID> cacheDistribution(int cacheId) {
        return cachePartitionsDistribution.get(cacheId);
    }
}
