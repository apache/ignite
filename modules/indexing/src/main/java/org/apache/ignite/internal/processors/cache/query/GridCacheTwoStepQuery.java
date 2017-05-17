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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Two step map-reduce style query.
 */
public class GridCacheTwoStepQuery {
    /** */
    public static final int DFLT_PAGE_SIZE = 1000;

    /** */
    @GridToStringInclude
    private List<GridCacheSqlQuery> mapQrys = new ArrayList<>();

    /** */
    @GridToStringInclude
    private GridCacheSqlQuery rdc;

    /** */
    private int pageSize = DFLT_PAGE_SIZE;

    /** */
    private boolean explain;

    /** */
    private String originalSql;

    /** */
    private Set<QueryTable> tbls;

    /** */
    private boolean distributedJoins;

    /** */
    private boolean skipMergeTbl;

    /** */
    private List<Integer> cacheIds;

    /** */
    private boolean local;

    /**
     * @param originalSql Original query SQL.
     * @param tbls Tables in query.
     */
    public GridCacheTwoStepQuery(String originalSql, Set<QueryTable> tbls) {
        this.originalSql = originalSql;
        this.tbls = tbls;
    }

    /**
     * Specify if distributed joins are enabled for this query.
     *
     * @param distributedJoins Distributed joins enabled.
     */
    public void distributedJoins(boolean distributedJoins) {
        this.distributedJoins = distributedJoins;
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
     * @param skipMergeTbl Skip merge table.
     */
    public void skipMergeTable(boolean skipMergeTbl) {
        this.skipMergeTbl = skipMergeTbl;
    }

    /**
     * @return If this is explain query.
     */
    public boolean explain() {
        return explain;
    }

    /**
     * @param explain If this is explain query.
     */
    public void explain(boolean explain) {
        this.explain = explain;
    }

    /**
     * @param pageSize Page size.
     */
    public void pageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * @param qry SQL Query.
     */
    public void addMapQuery(GridCacheSqlQuery qry) {
        mapQrys.add(qry);
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
     * @param rdc Reduce query.
     */
    public void reduceQuery(GridCacheSqlQuery rdc) {
        this.rdc = rdc;
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
     * @param cacheIds Cache IDs.
     */
    public void cacheIds(List<Integer> cacheIds) {
        this.cacheIds = cacheIds;
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
     * @param local Local query flag.
     */
    public void local(boolean local) {
        this.local = local;
    }

    /**
     * @return Copy.
     */
    public GridCacheTwoStepQuery copy() {
        assert !explain;

        GridCacheTwoStepQuery cp = new GridCacheTwoStepQuery(originalSql, tbls);

        cp.cacheIds = cacheIds;
        cp.rdc = rdc.copy();
        cp.skipMergeTbl = skipMergeTbl;
        cp.pageSize = pageSize;
        cp.distributedJoins = distributedJoins;

        for (int i = 0; i < mapQrys.size(); i++)
            cp.mapQrys.add(mapQrys.get(i).copy());

        return cp;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTwoStepQuery.class, this);
    }
}
