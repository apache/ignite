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

import javax.cache.Cache;
import org.apache.ignite.internal.cache.query.IndexCondition;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteExperimental;
import org.jetbrains.annotations.Nullable;

/**
 * Index query runs over internal index structure and returns cache entries for index rows that match specified condition.
 */
@IgniteExperimental
public class IndexQuery<K, V> extends Query<Cache.Entry<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Index condition describes index query clause. */
    private IndexCondition idxCond;

    /** Cache Value class. Describes a table within a cache that runs a query. */
    private final String valCls;

    /** Optional index name. Find index by fields in condition. */
    private final @Nullable String idxName;

    /** Optional schema name. User has to specify schema to run query over an index created with SQL. */
    private final @Nullable String schema;

    /** */
    private IndexQuery(String valCls, @Nullable String idxName, @Nullable String schema) {
        this.valCls = valCls;
        this.idxName = idxName;
        this.schema = schema;
    }

    /**
     * Specify index with cache value class. Ignite checks all indexes to find best match by
     * {@link #valCls} and {@link IndexCondition#fields()}.
     */
    public static <K, V> IndexQuery<K, V> forType(Class<V> valCls) {
        A.notNull(valCls, "valCls");

        return new IndexQuery<K, V>(valCls.getName(), null, null);
    }

    /**
     * Specify index with cache value class and index name.
     */
    public static <K, V> IndexQuery<K, V> forIndex(Class<V> valCls, String idxName) {
        A.notNull(valCls, "valCls");
        A.notNullOrEmpty(idxName, "idxName");

        return new IndexQuery<K, V>(valCls.getName(), idxName, null);
    }

    /**
     * Specify index with cache value class, index name and schema name.
     * Note that schema is required parameter for indexes created with the "CREATE INDEX" SQL-clause.
     */
    public static <K, V> IndexQuery<K, V> forIndex(Class<V> valCls, String idxName, String schema) {
        A.notNull(valCls, "valCls");
        A.notNullOrEmpty(idxName, "idxName");
        A.notNullOrEmpty(schema, "schema");

        return new IndexQuery<K, V>(valCls.getName(), idxName, schema);
    }

    /**
     * Provide index condition to query an index.
     */
    public IndexQuery<K, V> where(IndexCondition idxCond) {
        A.notNull(idxCond, "idxCond");

        this.idxCond = idxCond;

        return this;
    }

    /**
     * Provide multiple index conditions. Order of conditons has to match index structure.
     */
    public IndexQuery<K, V> where(IndexCondition... idxConds) {
        A.ensure(idxConds.length > 1, "Expect multiple index conditions.");

        for (IndexCondition c: idxConds) {
            A.notNull(c, "idxConds");

            if (idxCond == null)
                idxCond = c;
            else
                idxCond.and(c);
        }

        return this;
    }

    /** Index condition. */
    public IndexCondition getIndexCondition() {
        return idxCond;
    }

    /** Cache value class. */
    public String getValueClass() {
        return valCls;
    }

    /** Index name. */
    public @Nullable String getIndexName() {
        return idxName;
    }

    /** Schema name. */
    public @Nullable String getSchema() {
        return schema;
    }
}
