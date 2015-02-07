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

import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.indexing.*;

import java.io.*;

/**
 * Query to pass into any of {@code Cache.query(...)} methods.
 * Use {@link QuerySqlPredicate} and {@link QueryTextPredicate} for SQL and
 * text queries accordingly.
 */
public abstract class QueryPredicate<T extends QueryPredicate> implements Serializable {
    /** Page size. */
    private int pageSize;

    /**
     * Empty constructor.
     */
    protected QueryPredicate() {
        // No-op.
    }

    /**
     * Factory method for SQL queries.
     *
     * @param sql SQL Query string.
     * @return SQL Query instance.
     */
    public static QuerySqlPredicate sql(String sql) {
        return new QuerySqlPredicate(sql);
    }

    /**
     * Factory method for SQL queries.
     *
     * @param type Type to be queried.
     * @param sql SQL Query string.
     * @return SQL Query instance.
     */
    public static QuerySqlPredicate sql(Class<?> type, String sql) {
        return sql(sql).setType(type);
    }

    /**
     * Factory method for Lucene fulltext queries.
     *
     * @param txt Search string.
     * @return Fulltext query.
     */
    public static QueryTextPredicate text(String txt) {
        return new QueryTextPredicate(txt);
    }

    /**
     * Factory method for Lucene fulltext queries.
     *
     * @param type Type to be queried.
     * @param txt Search string.
     * @return Fulltext query.
     */
    public static QueryTextPredicate text(Class<?> type, String txt) {
        return text(txt).setType(type);
    }

    /**
     * Factory method for SPI queries.
     *
     * @param filter Filter.
     * @return SPI Query.
     */
    public static <K, V> QueryScanPredicate<K, V> scan(final IgniteBiPredicate<K, V> filter) {
        return new QueryScanPredicate<>(filter);
    }

    /**
     * Factory method for SPI queries returning all entries.
     *
     * @return SPI Query.
     */
    public static <K, V> QueryScanPredicate<K, V> scan() {
        return new QueryScanPredicate<>();
    }

    /**
     * Factory method for SPI queries.
     *
     * @return SPI Query.
     * @see IndexingSpi
     */
    public static QuerySpiPredicate spi() {
        return new QuerySpiPredicate();
    }

    /**
     * Gets optional page size, if {@code 0}, then {@link CacheQueryConfiguration#getPageSize()} is used.
     *
     * @return Optional page size.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Sets optional page size, if {@code 0}, then {@link CacheQueryConfiguration#getPageSize()} is used.
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
        return S.toString(QueryPredicate.class, this);
    }
}
