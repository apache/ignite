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

import java.io.*;
import java.util.*;

/**
 * Query configuration object.
 */
public class QueryConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Collection of query type metadata. */
    private Collection<QueryTypeMetadata> typeMeta;

    /** Query type resolver. */
    private QueryTypeResolver typeRslvr;

    /** */
    private boolean idxPrimitiveKey;

    /** */
    private boolean idxPrimitiveVal;

    /** */
    private boolean idxFixedTyping;

    /** */
    private boolean escapeAll;

    /** */
    private int pageSize = 1000;

    /**
     * Default constructor.
     */
    public QueryConfiguration() {
        // No-op.
    }

    /**
     * @param cfg Configuration to copy.
     */
    public QueryConfiguration(QueryConfiguration cfg) {
        typeMeta = cfg.getTypeMetadata();
        typeRslvr = cfg.getTypeResolver();
        idxPrimitiveKey = cfg.isIndexPrimitiveKey();
        idxPrimitiveVal = cfg.isIndexPrimitiveValue();
        idxFixedTyping = cfg.isIndexFixedTyping();
        escapeAll = cfg.isEscapeAll();
    }

    /**
     * Gets collection of query type metadata objects.
     *
     * @return Collection of query type metadata.
     */
    public Collection<QueryTypeMetadata> getTypeMetadata() {
        return typeMeta;
    }

    /**
     * Sets collection of query type metadata objects.
     *
     * @param typeMeta Collection of query type metadata.
     */
    public void setTypeMetadata(Collection<QueryTypeMetadata> typeMeta) {
        this.typeMeta = typeMeta;
    }

    /**
     * Gets query type resolver.
     *
     * @return Query type resolver.
     */
    public QueryTypeResolver getTypeResolver() {
        return typeRslvr;
    }

    /**
     * Sets query type resolver.
     *
     * @param typeRslvr Query type resolver.
     */
    public void setTypeResolver(QueryTypeResolver typeRslvr) {
        this.typeRslvr = typeRslvr;
    }

    /**
     * Gets flag indicating whether SQL engine should index by key in cases
     * where key is primitive type
     *
     * @return {@code True} if primitive keys should be indexed.
     */
    public boolean isIndexPrimitiveKey() {
        return idxPrimitiveKey;
    }

    /**
     * Sets flag indicating whether SQL engine should index by key in cases
     * where key is primitive type.
     *
     * @param idxPrimitiveKey {@code True} if primitive keys should be indexed.
     */
    public void setIndexPrimitiveKey(boolean idxPrimitiveKey) {
        this.idxPrimitiveKey = idxPrimitiveKey;
    }

    /**
     * Gets flag indicating whether SQL engine should index by value in cases
     * where value is primitive type
     *
     * @return {@code True} if primitive values should be indexed.
     */
    public boolean isIndexPrimitiveValue() {
        return idxPrimitiveVal;
    }

    /**
     * Sets flag indexing whether SQL engine should index by value in cases
     * where value is primitive type.
     *
     * @param idxPrimitiveVal {@code True} if primitive values should be indexed.
     */
    public void setIndexPrimitiveValue(boolean idxPrimitiveVal) {
        this.idxPrimitiveVal = idxPrimitiveVal;
    }

    /**
     * This flag essentially controls whether all values of the same type have
     * identical key type.
     * <p>
     * If {@code false}, SQL engine will store all keys in BINARY form to make it possible to store
     * the same value type with different key types. If {@code true}, key type will be converted
     * to respective SQL type if it is possible, hence, improving performance of queries.
     * <p>
     * Setting this value to {@code false} also means that {@code '_key'} column cannot be indexed and
     * cannot participate in query where clauses. The behavior of using '_key' column in where
     * clauses with this flag set to {@code false} is undefined.
     *
     * @return {@code True} if SQL engine should try to convert values to their respective SQL
     *      types for better performance.
     */
    public boolean isIndexFixedTyping() {
        return idxFixedTyping;
    }

    /**
     * This flag essentially controls whether key type is going to be identical
     * for all values of the same type.
     * <p>
     * If false, SQL engine will store all keys in BINARY form to make it possible to store
     * the same value type with different key types. If true, key type will be converted
     * to respective SQL type if it is possible, which may provide significant performance
     * boost.
     *
     * @param idxFixedTyping {@code True} if SQL engine should try to convert values to their respective SQL
     *      types for better performance.
     */
    public void setIndexFixedTyping(boolean idxFixedTyping) {
        this.idxFixedTyping = idxFixedTyping;
    }

    /**
     * If {@code true}, then table name and all column names in 'create table' SQL
     * generated for SQL engine are escaped with double quotes. This flag should be set if table name of
     * column name is H2 reserved word or is not valid H2 identifier (e.g. contains space or hyphen).
     * <p>
     * Note if this flag is set then table and column name in SQL queries also must be escaped with double quotes.

     * @return Flag value.
     */
    public boolean isEscapeAll() {
        return escapeAll;
    }

    /**
     * If {@code true}, then table name and all column names in 'create table' SQL
     * generated for SQL engine are escaped with double quotes. This flag should be set if table name of
     * column name is H2 reserved word or is not valid H2 identifier (e.g. contains space or hyphen).
     * <p>
     * Note if this flag is set then table and column name in SQL queries also must be escaped with double quotes.

     * @param escapeAll Flag value.
     */
    public void setEscapeAll(boolean escapeAll) {
        this.escapeAll = escapeAll;
    }

    /**
     * Gets default query result page size.
     *
     * @return Page size.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Sets default query result page size.
     *
     * @param pageSize Page size.
     */
    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }
}
