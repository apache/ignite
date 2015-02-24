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

package org.apache.ignite.cache.query;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;

/**
 * Base class for all Ignite cache queries.
 * Use {@link SqlQuery} and {@link TextQuery} for SQL and
 * text queries accordingly.
 *
 * @see IgniteCache#query(Query)
 * @see IgniteCache#localQuery(Query)
 * @see IgniteCache#queryFields(SqlFieldsQuery)
 * @see IgniteCache#localQueryFields(SqlFieldsQuery)
 */
public abstract class Query<T extends Query> implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Page size. */
    private int pageSize;

    /**
     * Empty constructor.
     */
    Query() {
        // No-op.
    }

    /**
     * Gets optional page size, if {@code 0}, then default is used.
     *
     * @return Optional page size.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Sets optional page size, if {@code 0}, then default is used.
     *
     * @param pageSize Optional page size.
     * @return {@code this} For chaining.
     */
    @SuppressWarnings("unchecked")
    public T setPageSize(int pageSize) {
        this.pageSize = pageSize;

        return (T)this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Query.class, this);
    }
}
