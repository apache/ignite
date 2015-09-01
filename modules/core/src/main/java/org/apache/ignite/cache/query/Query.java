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

import java.io.Serializable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Base class for all Ignite cache queries.
 * Use {@link SqlQuery} and {@link TextQuery} for SQL and
 * text queries accordingly.
 *
 * @see IgniteCache#query(Query)
 */
public abstract class Query<R> implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default query page size. */
    public static final int DFLT_PAGE_SIZE = 1024;

    /** Page size. */
    private int pageSize = DFLT_PAGE_SIZE;

    /** Local flag. */
    private boolean loc;

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
     * @return {@code this} for chaining.
     */
    public Query<R> setPageSize(int pageSize) {
        if (pageSize <= 0)
            throw new IllegalArgumentException("Page size must be above zero.");

        this.pageSize = pageSize;

        return this;
    }

    /**
     * Returns {@code true} if this query should be executed on local node only.
     *
     * @return Local flag.
     */
    public boolean isLocal() {
        return loc;
    }

    /**
     * Sets whether this query should be executed on local node only.
     *
     * @param loc Local flag.
     * @return {@code this} for chaining.
     */
    public Query<R> setLocal(boolean loc) {
        this.loc = loc;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Query.class, this);
    }
}