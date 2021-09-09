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
package org.apache.ignite.cache;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Contains list of fields to be indexed. It is possible to provide field name
 * suffixed with index specific extension, for example for {@link QueryIndexType#SORTED sorted} index
 * the list can be provided as following {@code (id, name asc, age desc)}.
 */
@SuppressWarnings("TypeMayBeWeakened")
public class QueryIndex implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final QueryIndexType DFLT_IDX_TYP = QueryIndexType.SORTED;

    /** Default index inline size. */
    public static final int DFLT_INLINE_SIZE = -1;

    /** Index name. */
    private String name;

    /** */
    @GridToStringInclude
    private LinkedHashMap<String, IndexFieldOrder> fields;

    /** */
    private QueryIndexType type = DFLT_IDX_TYP;

    /** */
    private int inlineSize = DFLT_INLINE_SIZE;

    /**
     * Creates an empty index. Should be populated via setters.
     */
    public QueryIndex() {
        // Empty constructor.
    }

    /**
     * Creates single-field sorted ascending index.
     *
     * @param field Field name.
     */
    public QueryIndex(String field) {
        this(field, QueryIndexType.SORTED, true);
    }

    /**
     * Creates single-field sorted index.
     *
     * @param field Field name.
     * @param asc Ascending flag.
     */
    public QueryIndex(String field, boolean asc) {
        this(field, QueryIndexType.SORTED, asc);
    }

    /**
     * Creates single-field sorted index.
     *
     * @param field Field name.
     * @param asc Ascending flag.
     * @param name Index name.
     */
    public QueryIndex(String field, boolean asc, String name) {
        this(field, QueryIndexType.SORTED, asc, name);
    }

    /**
     * Creates index for one field.
     * If index is sorted, then ascending sorting is used by default.
     * To specify sort order, use the next method.
     * This constructor should also have a corresponding setter method.
     *
     * @param field Field name.
     * @param type Index type.
     */
    public QueryIndex(String field, QueryIndexType type) {
        this(field, type, true);
    }

    /**
     * Creates index for one field. The last boolean parameter is ignored for non-sorted indexes.
     *
     * @param field Field name.
     * @param type Index type.
     * @param asc Ascending flag.
     */
    public QueryIndex(String field, QueryIndexType type, boolean asc) {
        this(field, type, new IndexFieldOrder(asc), null);
    }

    /**
     * Creates index for one field. The last boolean parameter is ignored for non-sorted indexes.
     *
     * @param field Field name.
     * @param type Index type.
     * @param asc Ascending flag.
     * @param name Index name.
     */
    public QueryIndex(String field, QueryIndexType type, boolean asc, String name) {
        this(field, type, new IndexFieldOrder(asc), name);
    }

    /**
     * Creates index for one field. The last boolean parameter is ignored for non-sorted indexes.
     *
     * @param field Field name.
     * @param type Index type.
     * @param order Field order.
     */
    public QueryIndex(String field, QueryIndexType type, IndexFieldOrder order) {
        this(field, type, order, null);
    }

    /**
     * Creates index for one field. The last boolean parameter is ignored for non-sorted indexes.
     *
     * @param field Field name.
     * @param type Index type.
     * @param order Field order.
     * @param name Index name.
     */
    public QueryIndex(String field, QueryIndexType type, IndexFieldOrder order, String name) {
        fields = new LinkedHashMap<>();

        fields.put(field, order);

        this.type = type;
        this.name = name;
    }

    /**
     * Creates index for a collection of fields. If index is sorted, fields will be sorted in
     * ascending order.
     *
     * @param fields Collection of fields to create an index.
     * @param type Index type.
     */
    public QueryIndex(Collection<String> fields, QueryIndexType type) {
        this.fields = new LinkedHashMap<>();

        for (String field : fields)
            this.fields.put(field, new IndexFieldOrder());

        this.type = type;
    }

    /**
     * Creates index for a collection of fields. The order of fields in the created index will be the same
     * as iteration order in the passed map. Map value defines whether the index will be ascending.
     *
     * @param fields Field name to field sort direction for sorted indexes.
     * @param type Index type.
     */
    public QueryIndex(LinkedHashMap<String, Boolean> fields, QueryIndexType type) {
        setFields(fields);

        this.type = type;
    }

    /**
     * Gets index name. Will be automatically set if not provided by a user.
     *
     * @return Index name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets index name.
     *
     * @param name Index name.
     * @return {@code this} for chaining.
     */
    public QueryIndex setName(String name) {
        this.name = name;

        return this;
    }

    /**
     * Gets fields included in the index.
     *
     * @return Collection of index fields.
     * @deprecated Use {@link #getFieldsOrder()} instead.
     */
    @Deprecated
    public LinkedHashMap<String, Boolean> getFields() {
        LinkedHashMap<String, Boolean> ret = new LinkedHashMap<>();

        for (Map.Entry<String, IndexFieldOrder> f: fields.entrySet())
            ret.put(f.getKey(), f.getValue().isAscending());

        return ret;
    }

    /**
     * Gets fields included in the index.
     *
     * @return Collection of index fields.
     */
    public LinkedHashMap<String, IndexFieldOrder> getFieldsOrder() {
        return new LinkedHashMap<>(fields);
    }

    /**
     * Sets fields included in the index.
     *
     * @param fields Collection of index fields.
     * @return {@code this} for chaining.
     * @deprecated Use {@link #setFieldsOrder(LinkedHashMap)} instead.
     */
    @Deprecated
    public QueryIndex setFields(LinkedHashMap<String, Boolean> fields) {
        this.fields = new LinkedHashMap<>();

        for (Map.Entry<String, Boolean> f: fields.entrySet())
            this.fields.put(f.getKey(), new IndexFieldOrder(f.getValue()));

        return this;
    }

    /**
     * Sets fields included in the index.
     *
     * @param fields Collection of index fields.
     * @return {@code this} for chaining.
     */
    public QueryIndex setFieldsOrder(LinkedHashMap<String, IndexFieldOrder> fields) {
        this.fields = new LinkedHashMap<>(fields);

        return this;
    }

    /**
     * Sets fields included in the index.
     *
     * @param field Index field.
     * @return {@code this} for chaining.
     */
    public QueryIndex addField(String field, IndexFieldOrder order) {
        if (fields == null)
            fields = new LinkedHashMap<>();

        fields.put(field, order);

        return this;
    }

    /**
     * @return Gets a collection of field names.
     */
    public Collection<String> getFieldNames() {
        return fields.keySet();
    }

    /**
     * Sets a collection of field names altogether with the field sorting direction. Sorting direction will be
     * ignored for non-sorted indexes.
     *
     * @param fields Collection of fields.
     * @param asc Ascending flag.
     * @return {@code this} for chaining.
     * @deprecated Use {@link #setFieldNames(Collection, IndexFieldOrder)} instead.
     */
    @Deprecated
    public QueryIndex setFieldNames(Collection<String> fields, boolean asc) {
        this.fields = new LinkedHashMap<>();

        for (String field : fields)
            this.fields.put(field, new IndexFieldOrder(asc));

        return this;
    }

    /**
     * Sets a collection of field names altogether with the field sorting direction. Sorting direction will be
     * ignored for non-sorted indexes.
     *
     * @param fields Collection of fields.
     * @param order Order.
     * @return {@code this} for chaining.
     */
    public QueryIndex setFieldNames(Collection<String> fields, IndexFieldOrder order) {
        this.fields = new LinkedHashMap<>();

        for (String field : fields)
            this.fields.put(field, order);

        return this;
    }

    /**
     * Gets index type.
     *
     * @return Index type.
     */
    public QueryIndexType getIndexType() {
        return type;
    }

    /**
     * Sets index type.
     *
     * @param type Index type.
     * @return {@code this} for chaining.
     */
    public QueryIndex setIndexType(QueryIndexType type) {
        this.type = type;

        return this;
    }

    /**
     * Gets index inline size in bytes. When enabled part of indexed value will be placed directly to index pages,
     * thus minimizing data page accesses, thus increasing query performance.
     * <p>
     * Allowed values:
     * <ul>
     *     <li>{@code -1} (default) - determine inline size automatically (see below)</li>
     *     <li>{@code 0} - index inline is disabled (not recommended)</li>
     *     <li>positive value - fixed index inline</li>
     * </ul>
     * When set to {@code -1}, Ignite will try to detect inline size automatically. It will be no more than
     * {@link CacheConfiguration#getSqlIndexMaxInlineSize()}. Index inline will be enabled for all fixed-length types,
     * but <b>will not be enabled</b> for {@code String}.
     *
     * @return Index inline size in bytes.
     */
    public int getInlineSize() {
        return inlineSize;
    }

    /**
     * Sets index inline size in bytes. When enabled part of indexed value will be placed directly to index pages,
     * thus minimizing data page accesses, thus increasing query performance.
     * <p>
     * Allowed values:
     * <ul>
     *     <li>{@code -1} (default) - determine inline size automatically (see below)</li>
     *     <li>{@code 0} - index inline is disabled (not recommended)</li>
     *     <li>positive value - fixed index inline</li>
     * </ul>
     * When set to {@code -1}, Ignite will try to detect inline size automatically. It will be no more than
     * {@link CacheConfiguration#getSqlIndexMaxInlineSize()}. Index inline will be enabled for all fixed-length types,
     * but <b>will not be enabled</b> for {@code String}.
     *
     * @param inlineSize Inline size.
     * @return {@code this} for chaining.
     */
    public QueryIndex setInlineSize(int inlineSize) {
        this.inlineSize = inlineSize;

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        QueryIndex idx = (QueryIndex)o;

        return inlineSize == idx.inlineSize &&
            F.eq(name, idx.name) &&
            F.eq(fields, idx.fields) &&
            type == idx.type;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(name, fields, type, inlineSize);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryIndex.class, this);
    }
}
