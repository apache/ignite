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

import org.apache.ignite.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Two step map-reduce style query.
 */
public class GridCacheTwoStepQuery implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final int DFLT_PAGE_SIZE = 1000;

    /** */
    @GridToStringInclude
    private Map<String, GridCacheSqlQuery> mapQrys;

    /** */
    @GridToStringInclude
    private GridCacheSqlQuery reduce;

    /** */
    private int pageSize = DFLT_PAGE_SIZE;

    /**
     * @param qry Reduce query.
     * @param params Reduce query parameters.
     */
    public GridCacheTwoStepQuery(String qry, Object ... params) {
        reduce = new GridCacheSqlQuery(null, qry, params);
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
     * @param alias Alias.
     * @param qry SQL Query.
     * @param params Query parameters.
     */
    public void addMapQuery(String alias, String qry, Object ... params) {
        A.ensure(!F.isEmpty(alias), "alias must not be empty");

        if (mapQrys == null)
            mapQrys = new GridLeanMap<>();

        if (mapQrys.put(alias, new GridCacheSqlQuery(alias, qry, params)) != null)
            throw new IgniteException("Failed to add query, alias already exists: " + alias + ".");
    }

    /**
     * @return Reduce query.
     */
    public GridCacheSqlQuery reduceQuery() {
        return reduce;
    }

    /**
     * @return Map queries.
     */
    public Collection<GridCacheSqlQuery> mapQueries() {
        return new ArrayList<>(mapQrys.values()); // Copy to make it Serializable.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTwoStepQuery.class, this);
    }
}
