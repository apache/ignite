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

package org.apache.ignite.springdata20.repository.query;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;

import java.lang.reflect.Method;

import static org.apache.ignite.springdata20.repository.query.QueryUtils.calcReturnType;
import static org.apache.ignite.springdata20.repository.query.QueryUtils.prepareQuery;
import static org.apache.ignite.springdata20.repository.query.QueryUtils.transformQueryCursor;

/**
 * Ignite SQL query implementation.
 */
@SuppressWarnings({"rawtypes"})
public class IgniteRepositoryQuery implements RepositoryQuery {
    /** Type. */
    private final Class<?> type;

    /** Sql. */
    private final IgniteQuery qry;

    /** Cache. */
    private final IgniteCache cache;

    /** Method. */
    private final Method mtd;

    /** Metadata. */
    private final RepositoryMetadata metadata;

    /** Factory. */
    private final ProjectionFactory factory;

    /** Return strategy. */
    private final ReturnStrategy returnStgy;

    /**
     * @param metadata Metadata.
     * @param qry Query.
     * @param mtd Method.
     * @param factory Factory.
     * @param cache Cache.
     */
    public IgniteRepositoryQuery(RepositoryMetadata metadata, IgniteQuery qry,
        Method mtd, ProjectionFactory factory, IgniteCache cache) {
        type = metadata.getDomainType();
        this.qry = qry;
        this.cache = cache;
        this.metadata = metadata;
        this.mtd = mtd;
        this.factory = factory;

        returnStgy = calcReturnType(mtd, qry.isFieldQuery());
    }

    /** {@inheritDoc} */
    @Override public Object execute(Object[] prmtrs) {
        Query qry = prepareQuery(this.qry, prmtrs);

        try (QueryCursor qryCursor = cache.query(qry)) {
            return transformQueryCursor(qryCursor, prmtrs, this.qry.isFieldQuery(), returnStgy);
        }
    }

    /** {@inheritDoc} */
    @Override public QueryMethod getQueryMethod() {
        return new QueryMethod(mtd, metadata, factory);
    }
}
