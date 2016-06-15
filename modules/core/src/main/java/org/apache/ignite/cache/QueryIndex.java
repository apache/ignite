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

/**
 * Contains list of fields to be indexed. It is possible to provide field name
 * suffixed with index specific extension, for example for {@link QueryIndexType#SORTED sorted} index
 * the list can be provided as following {@code (id, name asc, age desc)}.
 */
@SuppressWarnings("TypeMayBeWeakened")
public class QueryIndex implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Index name. */
    private String name;

    /** */
    private LinkedHashMap<String, Boolean> fields;

    /** */
    private QueryIndexType type;

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
        this(field, QueryIndexType.SORTED, asc);

        this.name = name;
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
        fields = new LinkedHashMap<>();
        fields.put(field, asc);

        this.type = type;
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
        fields = new LinkedHashMap<>();
        fields.put(field, asc);

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
            this.fields.put(field, true);

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
        this.fields = fields;
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
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets fields included in the index.
     *
     * @return Collection of index fields.
     */
    public LinkedHashMap<String, Boolean> getFields() {
        return fields;
    }

    /**
     * Sets fields included in the index.
     *
     * @param fields Collection of index fields.
     */
    public void setFields(LinkedHashMap<String, Boolean> fields) {
        this.fields = fields;
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
     */
    public void setFieldNames(Collection<String> fields, boolean asc) {
        this.fields = new LinkedHashMap<>();

        for (String field : fields)
            this.fields.put(field, asc);
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
     */
    public void setIndexType(QueryIndexType type) {
        this.type = type;
    }
}
