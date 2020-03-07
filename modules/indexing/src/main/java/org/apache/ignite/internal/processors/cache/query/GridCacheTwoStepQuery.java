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

import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionResult;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Two step map-reduce style query.
 */
public class GridCacheTwoStepQuery {
    /** */
    @GridToStringInclude
    private final List<GridCacheSqlQuery> mapQrys;

    /** */
    @GridToStringInclude
    private final GridCacheSqlQuery rdc;

    /** */
    private final boolean explain;

    /** */
    private final String originalSql;

    /** */
    private final Set<QueryTable> tbls;

    /** */
    private final boolean distributedJoins;

    /** */
    private final boolean replicatedOnly;

    /** */
    private final boolean skipMergeTbl;

    /** */
    private final List<Integer> cacheIds;

    /** */
    private final boolean locSplit;

    /** */
    private final PartitionResult derivedPartitions;

    /** */
    private final boolean mvccEnabled;

    /** Number of positional arguments in the sql. */
    private final int paramsCnt;

    /**
     *
     * @param originalSql Original SQL.
     * @param paramsCnt Parameters count.
     * @param tbls Tables.
     * @param rdc Reduce query.
     * @param mapQrys Map query.
     * @param skipMergeTbl Skip merge table flag.
     * @param explain Explain flag.
     * @param distributedJoins Distributed joins flag.
     * @param replicatedOnly Replicated only flag.
     * @param derivedPartitions Derived partitions.
     * @param cacheIds Cache ids.
     * @param mvccEnabled Mvcc flag.
     * @param locSplit Local split flag.
     */
    public GridCacheTwoStepQuery(
        String originalSql,
        int paramsCnt,
        Set<QueryTable> tbls,
        GridCacheSqlQuery rdc,
        List<GridCacheSqlQuery> mapQrys,
        boolean skipMergeTbl,
        boolean explain,
        boolean distributedJoins,
        boolean replicatedOnly,
        PartitionResult derivedPartitions,
        List<Integer> cacheIds,
        boolean mvccEnabled,
        boolean locSplit
    ) {
        assert !F.isEmpty(mapQrys);

        this.originalSql = originalSql;
        this.paramsCnt = paramsCnt;
        this.tbls = tbls;
        this.rdc = rdc;
        this.skipMergeTbl = skipMergeTbl;
        this.explain = explain;
        this.distributedJoins = distributedJoins;
        this.derivedPartitions = derivedPartitions;
        this.cacheIds = cacheIds;
        this.mvccEnabled = mvccEnabled;
        this.locSplit = locSplit;
        this.mapQrys = mapQrys;
        this.replicatedOnly = replicatedOnly;
    }

    /**
     * Check if distributed joins are enabled for this query.
     *
     * @return {@code true} If distributed joins enabled.
     */
    public boolean distributedJoins() {
        return distributedJoins;
    }

    /**
     * @return {@code True} if reduce query can skip merge table creation and get data directly from merge index.
     */
    public boolean skipMergeTable() {
        return skipMergeTbl;
    }

    /**
     * @return If this is explain query.
     */
    public boolean explain() {
        return explain;
    }

    /**
     * @return {@code true} If all the map queries contain only replicated tables.
     */
    public boolean isReplicatedOnly() {
        assert !mapQrys.isEmpty();

        return replicatedOnly;
    }

    /**
     * @return Reduce query.
     */
    public GridCacheSqlQuery reduceQuery() {
        return rdc;
    }

    /**
     * @return Map queries.
     */
    public List<GridCacheSqlQuery> mapQueries() {
        return mapQrys;
    }

    /**
     * @return Cache IDs.
     */
    public List<Integer> cacheIds() {
        return cacheIds;
    }

    /**
     * @return Whether cache IDs exist.
     */
    public boolean hasCacheIds() {
        return !F.isEmpty(cacheIds);
    }

    /**
     * @return Original query SQL.
     */
    public String originalSql() {
        return originalSql;
    }

    /**
     * @return {@code True} If query is local.
     */
    public boolean isLocal() {
        return F.isEmpty(cacheIds) || locSplit;
    }

    /**
     * @return {@code True} if this is local query with split.
     */
    public boolean isLocalSplit() {
        return locSplit;
    }

    /**
     * @return Query derived partitions info.
     */
    public PartitionResult derivedPartitions() {
        return derivedPartitions;
    }

    /**
     * @return Nuumber of tables.
     */
    public int tablesCount() {
        return tbls.size();
    }

    /**
     * @return Tables.
     */
    public Set<QueryTable> tables() {
        return tbls;
    }

    /**
     * @return Mvcc flag.
     */
    public boolean mvccEnabled() {
        return mvccEnabled;
    }

    /**
     * @return Number of parameters
     */
    public int parametersCount() {
        return paramsCnt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTwoStepQuery.class, this);
    }
}
