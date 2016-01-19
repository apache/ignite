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
    private Set<String> spaces;

    /** */
    private final boolean skipMergeTbl;

    /**
     * @param spaces All spaces accessed in query.
     * @param rdc Reduce query.
     * @param skipMergeTbl {@code True} if reduce query can skip merge table creation and
     *      get data directly from merge index.
     */
    public GridCacheTwoStepQuery(Set<String> spaces, GridCacheSqlQuery rdc, boolean skipMergeTbl) {
        assert rdc != null;

        this.spaces = spaces;
        this.rdc = rdc;
        this.skipMergeTbl = skipMergeTbl;
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
     * @return {@code this}.
     */
    public GridCacheTwoStepQuery addMapQuery(GridCacheSqlQuery qry) {
        mapQrys.add(qry);

        return this;
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
     * @return Spaces.
     */
    public Set<String> spaces() {
        return spaces;
    }

    /**
     * @param spaces Spaces.
     */
    public void spaces(Set<String> spaces) {
        this.spaces = spaces;
    }

    /**
     * @param args New arguments to copy with.
     * @return Copy.
     */
    public GridCacheTwoStepQuery copy(Object[] args) {
        assert !explain;

        GridCacheTwoStepQuery cp = new GridCacheTwoStepQuery(spaces, rdc.copy(args), skipMergeTbl);
        cp.pageSize = pageSize;
        for (int i = 0; i < mapQrys.size(); i++)
            cp.mapQrys.add(mapQrys.get(i).copy(args));

        return cp;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTwoStepQuery.class, this);
    }
}