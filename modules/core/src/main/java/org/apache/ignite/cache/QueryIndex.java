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
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Objects;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.cache.query.QueryIndexMessage;
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

    /** Delegate */
    private final QueryIndexMessage impl;

    /**
     * Creates an index based on the implementation {@code impl}.
     *
     * @param delegate Th delegate.
     */
    public QueryIndex(QueryIndexMessage delegate) {
        impl = delegate;
    }

    /**
     * Creates an empty index. Should be populated via setters.
     */
    public QueryIndex() {
        impl = new QueryIndexMessage();

        impl.type = DFLT_IDX_TYP;
        impl.inlineSize = DFLT_INLINE_SIZE;
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
        this(field, QueryIndexType.SORTED, asc);

        impl.name = name;
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
        this(Arrays.asList(field), type);
    }

    /**
     * Creates index for one field. The last boolean parameter is ignored for non-sorted indexes.
     *
     * @param field Field name.
     * @param type Index type.
     * @param asc Ascending flag.
     */
    public QueryIndex(String field, QueryIndexType type, boolean asc) {
        impl = new QueryIndexMessage();

        impl.inlineSize = DFLT_INLINE_SIZE;
        impl.type = type;

        impl.addField(field, asc);
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
        impl = new QueryIndexMessage();

        impl.inlineSize = DFLT_INLINE_SIZE;
        impl.type = type;
        impl.name = name;

        impl.addField(field, asc);
    }

    /**
     * Creates index for a collection of fields. If index is sorted, fields will be sorted in
     * ascending order.
     *
     * @param fields Collection of fields to create an index.
     * @param type Index type.
     */
    public QueryIndex(Collection<String> fields, QueryIndexType type) {
        impl = new QueryIndexMessage();

        impl.inlineSize = DFLT_INLINE_SIZE;
        impl.type = type;

        for (String field : fields)
            impl.addField(field, true);
    }

    /**
     * Creates index for a collection of fields. The order of fields in the created index will be the same
     * as iteration order in the passed map. Map value defines whether the index will be ascending.
     *
     * @param fields Field name to field sort direction for sorted indexes.
     * @param type Index type.
     */
    public QueryIndex(LinkedHashMap<String, Boolean> fields, QueryIndexType type) {
        impl = new QueryIndexMessage();

        impl.inlineSize = DFLT_INLINE_SIZE;
        impl.type = type;

        impl.fields = fields;
    }

    /**
     * Gets index name. Will be automatically set if not provided by a user.
     *
     * @return Index name.
     */
    public String getName() {
        return impl.name;
    }

    /**
     * Sets index name.
     *
     * @param name Index name.
     * @return {@code this} for chaining.
     */
    public QueryIndex setName(String name) {
        impl.name = name;

        return this;
    }

    /**
     * Gets the implementation.
     *
     * @return A delegate.
     */
    public QueryIndexMessage getDelegate() {
        return impl;
    }

    /**
     * Gets fields included in the index.
     *
     * @return Collection of index fields.
     */
    public LinkedHashMap<String, Boolean> getFields() {
        return impl.fields;
    }

    /**
     * Sets fields included in the index.
     *
     * @param fields Collection of index fields.
     * @return {@code this} for chaining.
     */
    public QueryIndex setFields(LinkedHashMap<String, Boolean> fields) {
        impl.fields = fields;

        return this;
    }

    /**
     * @return Gets a collection of field names.
     */
    public Collection<String> getFieldNames() {
        return impl.fields.keySet();
    }

    /**
     * Sets a collection of field names altogether with the field sorting direction. Sorting direction will be
     * ignored for non-sorted indexes.
     *
     * @param fields Collection of fields.
     * @param asc Ascending flag.
     * @return {@code this} for chaining.
     */
    public QueryIndex setFieldNames(Collection<String> fields, boolean asc) {
        impl.fields = new LinkedHashMap<>();

        for (String field : fields)
            impl.addField(field, asc);

        return this;
    }

    /**
     * Gets index type.
     *
     * @return Index type.
     */
    public QueryIndexType getIndexType() {
        return impl.type;
    }

    /**
     * Sets index type.
     *
     * @param type Index type.
     * @return {@code this} for chaining.
     */
    public QueryIndex setIndexType(QueryIndexType type) {
        impl.type = type;

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
        return impl.inlineSize;
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
        impl.inlineSize = inlineSize;

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        QueryIndex idx = (QueryIndex)o;

        return getInlineSize() == idx.getInlineSize() &&
            Objects.equals(getName(), idx.getName()) &&
            Objects.equals(getFields(), idx.getFields()) &&
            getIndexType() == idx.getIndexType();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(getName(), getFields(), getIndexType(), getInlineSize());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryIndex.class, this);
    }
}
