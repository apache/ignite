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

import java.util.Collections;
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
    private final boolean skipMergeTbl;

    /** */
    private final List<Integer> cacheIds;

    /** */
    private final boolean local;

    /** */
    private final PartitionResult derivedPartitions;

    /** */
    private final boolean mvccEnabled;

    /** {@code FOR UPDATE} flag. */
    private final boolean forUpdate;

    /** Number of positional arguments in the sql. */
    private final int paramsCnt;

    /**
     * @param originalSql Original query SQL.
     * @param tbls Tables in query.
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
        boolean forUpdate,
        PartitionResult derivedPartitions,
        List<Integer> cacheIds,
        boolean mvccEnabled,
        boolean local
    ) {
        this.originalSql = originalSql;
        this.paramsCnt = paramsCnt;
        this.tbls = tbls;
        this.rdc = rdc;
        this.mapQrys = F.isEmpty(mapQrys) ? Collections.emptyList() : mapQrys;
        this.skipMergeTbl = skipMergeTbl;
        this.explain = explain;
        this.distributedJoins = distributedJoins;
        this.forUpdate = forUpdate;
        this.derivedPartitions = derivedPartitions;
        this.cacheIds = cacheIds;
        this.mvccEnabled = mvccEnabled;
        this.local = local;
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

        for (GridCacheSqlQuery mapQry : mapQrys) {
            if (mapQry.isPartitioned())
                return false;
        }

        return true;
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
     * @return Original query SQL.
     */
    public String originalSql() {
        return originalSql;
    }

    /**
     * @return {@code True} If query is local.
     */
    public boolean isLocal() {
        return local;
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
     * @return {@code FOR UPDATE} flag.
     */
    public boolean forUpdate() {
        return forUpdate;
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
