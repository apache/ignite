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
import org.apache.ignite.internal.cache.query.RangeIndexCondition;
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

    /** Object to mark a boundary if {@code null} is specified. */
    private static final Object NULL = new Null();

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

        return new IndexQuery<>(valCls.getName(), null, null);
    }

    /**
     * Specify index with cache value class and index name.
     */
    public static <K, V> IndexQuery<K, V> forIndex(Class<V> valCls, String idxName) {
        A.notNull(valCls, "valCls");

        return new IndexQuery<>(valCls.getName(), idxName, null);
    }

    /**
     * Specify index with cache value class, index name and schema name.
     * Note that schema is required parameter for indexes created with the "CREATE INDEX" SQL-clause.
     */
    public static <K, V> IndexQuery<K, V> forIndex(Class<V> valCls, String idxName, String schema) {
        A.notNull(valCls, "valCls");

        return new IndexQuery<>(valCls.getName(), idxName, schema);
    }

    /** Less Then. */
    public IndexQuery<K, V> lt(String field, Object val) {
        A.ensure(idxCond == null, "The only index condition is supported.");

        RangeIndexCondition cond = new RangeIndexCondition();

        cond.addCondition(field, null, wrapNull(val));

        idxCond = cond;

        return this;
    }

    /** Less Then. */
    public IndexQuery<K, V> lt(String field, Object val, String field2, Object... vals) {
        A.ensure(idxCond == null, "The only index condition is supported.");

        A.notEmpty(vals, "vals");
        A.ensure(vals.length % 2 == 1, "number of fields has to be equal number of values.");

        RangeIndexCondition cond = new RangeIndexCondition();

        cond.addCondition(field, null, wrapNull(val));
        cond.addCondition(field2, null, wrapNull(vals[0]));

        for (int i = 1; i + 1 < vals.length; i += 2) {
            A.ensure(vals[i] instanceof String, "waited for field name but got " + vals[i]);

            cond.addCondition((String) vals[i], null, wrapNull(vals[i + 1]));
        }

        idxCond = cond;

        return this;
    }

    /** Less Then or Equal. */
    public IndexQuery<K, V> lte(String field, Object val) {
        lt(field, val);

        ((RangeIndexCondition) idxCond).upperInclusive(true);

        return this;
    }

    /** Less Then or Equal. */
    public IndexQuery<K, V> lte(String field, Object val, String field2, Object... vals) {
        lt(field, val, field2, vals);

        ((RangeIndexCondition) idxCond).upperInclusive(true);

        return this;
    }

    /** Greater Then. */
    public IndexQuery<K, V> gt(String field, Object val) {
        A.ensure(idxCond == null, "The only index condition is supported.");

        RangeIndexCondition cond = new RangeIndexCondition();

        cond.addCondition(field, wrapNull(val), null);

        idxCond = cond;

        return this;
    }

    /** Greater Then. */
    public IndexQuery<K, V> gt(String field, Object val, String field2, Object... vals) {
        A.ensure(idxCond == null, "The only index condition is supported.");

        A.notEmpty(vals, "vals");
        A.ensure(vals.length % 2 == 1, "number of fields has to be equal number of values.");

        RangeIndexCondition cond = new RangeIndexCondition();

        cond.addCondition(field, wrapNull(val), null);
        cond.addCondition(field2, wrapNull(vals[0]), null);

        for (int i = 1; i + 1 < vals.length; i += 2) {
            A.ensure(vals[i] instanceof String, "waited for field name but got " + vals[i]);

            cond.addCondition((String) vals[i], wrapNull(vals[i + 1]), null);
        }

        idxCond = cond;

        return this;
    }

    /** Greater Then or Equal. */
    public IndexQuery<K, V> gte(String field, Object val) {
        gt(field, val);

        ((RangeIndexCondition) idxCond).lowerInclusive(true);

        return this;
    }

    /** Greater Then or Equal. */
    public IndexQuery<K, V> gte(String field, Object val, String field2, Object... vals) {
        gt(field, val, field2, vals);

        ((RangeIndexCondition) idxCond).lowerInclusive(true);

        return this;
    }

    /** Between. Lower and upper boundaries are inclusive. */
    public IndexQuery<K, V> between(String field, Object lower, Object upper) {
        A.ensure(idxCond == null, "The only index condition is supported.");

        RangeIndexCondition cond = new RangeIndexCondition();

        cond.addCondition(field, wrapNull(lower), wrapNull(upper));

        cond.lowerInclusive(true);
        cond.upperInclusive(true);

        idxCond = cond;

        return this;
    }

    /** Between. Lower and upper boundaries are inclusive. */
    public IndexQuery<K, V> between(String field, Object lower, Object upper, String field2, Object... vals) {
        A.ensure(idxCond == null, "The only index condition is supported.");

        A.notEmpty(vals, "vals");
        A.ensure(vals.length % 2 == 0, "number of fields has to be equal number of values pairs.");

        RangeIndexCondition cond = new RangeIndexCondition();

        cond.addCondition(field, wrapNull(lower), wrapNull(upper));
        cond.addCondition(field2, wrapNull(vals[0]), wrapNull(vals[1]));

        for (int i = 2; i + 2 < vals.length; i += 3) {
            A.ensure(vals[i] instanceof String, "waited for field name but got " + vals[i]);

            cond.addCondition((String) vals[i], wrapNull(vals[i + 1]), wrapNull(vals[i + 2]));
        }

        cond.lowerInclusive(true);
        cond.upperInclusive(true);

        idxCond = cond;

        return this;
    }

    /** Index condition. */
    public IndexCondition idxCond() {
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

    /** */
    private Object wrapNull(Object val) {
        return val == null ? NULL : val;
    }

    /** Class to represent NULL value. */
    public static final class Null {}
}
