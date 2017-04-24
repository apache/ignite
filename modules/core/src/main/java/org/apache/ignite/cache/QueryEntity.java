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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Query entity is a description of {@link org.apache.ignite.IgniteCache cache} entry (composed of key and value)
 * in a way of how it must be indexed and can be queried.
 */
public class QueryEntity implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Key type. */
    private String keyType;

    /** Value type. */
    private String valType;

    /** Key name.
     * Can be used in field list to denote the key as a whole.
     * Default is _key.
     * */
    private String keyFieldName;

    /** Value name.
     * Can be used in field list to denote the entire value.
     * Default is _val.
     * */
    private String valueFieldName;

    /** Fields available for query. A map from field name to type name. */
    @GridToStringInclude
    private LinkedHashMap<String, String> fields = new LinkedHashMap<>();

    /** Set of field names that belong to the key. */
    @GridToStringInclude
    private Set<String> keyFields;

    /** Aliases. */
    @GridToStringInclude
    private Map<String, String> aliases = new HashMap<>();

    /** Collection of query indexes. */
    @GridToStringInclude
    private Collection<QueryIndex> idxs;

    /** Table name. */
    private String tableName;

    /**
     * Creates an empty query entity.
     */
    public QueryEntity() {
        // No-op constructor.
    }

    /**
     * Copy constructor.
     *
     * @param other Other entity.
     */
    public QueryEntity(QueryEntity other) {
        keyType = other.keyType;
        valType = other.valType;

        keyFieldName = other.keyFieldName;
        valueFieldName = other.valueFieldName;

        fields = new LinkedHashMap<>(other.fields);
        keyFields = other.keyFields != null ? new HashSet<>(other.keyFields) : null;

        aliases = new HashMap<>(other.aliases);
        idxs = other.idxs != null ? new ArrayList<>(other.idxs) : null;

        tableName = other.tableName;
    }

    /**
     * Creates a query entity with the given key and value types.
     *
     * @param keyType Key type.
     * @param valType Value type.
     */
    public QueryEntity(String keyType, String valType) {
        this.keyType = keyType;
        this.valType = valType;
    }

    /**
     * Gets key type for this query pair.
     *
     * @return Key type.
     */
    public String getKeyType() {
        return keyType;
    }

    /**
     * Attempts to get key type from fields
     * in case it was not set directly
     *
     * @return Value type.
     */
    public String findKeyType() {
        if (keyType != null)
            return keyType;

        if (fields != null && keyFieldName != null)
            return fields.get(keyFieldName);

        return null;
    }

    /**
     * Sets key type for this query pair.
     *
     * @param keyType Key type.
     * @return {@code this} for chaining.
     */
    public QueryEntity setKeyType(String keyType) {
        this.keyType = keyType;

        return this;
    }

    /**
     * Gets value type for this query pair.
     *
     * @return Value type.
     */
    public String getValueType() {
        return valType;
    }

    /**
     * Attempts to get value type from fields
     * in case it was not set directly
     *
     * @return Value type.
     */
    public String findValueType() {
        if (valType != null)
            return valType;

        if (fields != null && valueFieldName != null)
            return fields.get(valueFieldName);

        return null;
    }

    /**
     * Sets value type for this query pair.
     *
     * @param valType Value type.
     * @return {@code this} for chaining.
     */
    public QueryEntity setValueType(String valType) {
        this.valType = valType;

        return this;
    }

    /**
     * Gets query fields for this query pair. The order of fields is important as it defines the order
     * of columns returned by the 'select *' queries.
     *
     * @return Field-to-type map.
     */
    public LinkedHashMap<String, String> getFields() {
        return fields;
    }

    /**
     * Sets query fields for this query pair. The order if fields is important as it defines the
     * order of columns returned by the 'select *' queries.
     *
     * @param fields Field-to-type map.
     * @return {@code this} for chaining.
     */
    public QueryEntity setFields(LinkedHashMap<String, String> fields) {
        this.fields = fields;

        return this;
    }

    /**
     * Gets query fields for this query pair that belongs to the key. We need this for the cases when no key-value classes
     * are present on cluster nodes, and we need to build/modify keys and values during SQL DML operations.
     * Thus, setting this parameter in XML is not mandatory and should be based on particular use case.
     *
     * @return Set of names of key fields.
     */
    public Set<String> getKeyFields() {
        return keyFields;
    }

    /**
     * Gets query fields for this query pair that belongs to the key. We need this for the cases when no key-value classes
     * are present on cluster nodes, and we need to build/modify keys and values during SQL DML operations.
     * Thus, setting this parameter in XML is not mandatory and should be based on particular use case.
     *
     * @param keyFields Set of names of key fields.
     * @return {@code this} for chaining.
     */
    public QueryEntity setKeyFields(Set<String> keyFields) {
        this.keyFields = keyFields;

        return this;
    }

    /**
     * Gets key field name.
     * @return Key name.
     */
    public String getKeyFieldName() {
        return keyFieldName;
    }

    /**
     * Sets key field name.
     * @param keyFieldName Key name.
     */
    public void setKeyFieldName(String keyFieldName) {
        this.keyFieldName = keyFieldName;
    }

    /**
     * Get value field name.
     * @return Value name.
     */
    public String getValueFieldName() {
        return valueFieldName;
    }

    /**
     * Sets value field name.
     * @param valueFieldName value name.
     */
    public void setValueFieldName(String valueFieldName) {
        this.valueFieldName = valueFieldName;
    }

    /**
     * Gets a collection of index entities.
     *
     * @return Collection of index entities.
     */
    public Collection<QueryIndex> getIndexes() {
        return idxs == null ? Collections.<QueryIndex>emptyList() : idxs;
    }

    /**
     * Gets aliases map.
     *
     * @return Aliases.
     */
    public Map<String, String> getAliases() {
        return aliases;
    }

    /**
     * Sets mapping from full property name in dot notation to an alias that will be used as SQL column name.
     * Example: {"parent.name" -> "parentName"}.
     *
     * @param aliases Aliases map.
     * @return {@code this} for chaining.
     */
    public QueryEntity setAliases(Map<String, String> aliases) {
        this.aliases = aliases;

        return this;
    }

    /**
     * Sets a collection of index entities.
     *
     * @param idxs Collection of index entities.
     * @return {@code this} for chaining.
     */
    public QueryEntity setIndexes(Collection<QueryIndex> idxs) {
        this.idxs = idxs;

        return this;
    }

    /**
     * Gets table name for this query entity.
     *
     * @return table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Sets table name for this query entity.
     * @param tableName table name
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Utility method for building query entities programmatically.
     * @param fullName Full name of the field.
     * @param type Type of the field.
     * @param alias Field alias.
     * @return {@code this} for chaining.
     */
    public QueryEntity addQueryField(String fullName, String type, String alias) {
        A.notNull(fullName, "fullName");
        A.notNull(type, "type");

        fields.put(fullName, type);

        if (alias != null)
            aliases.put(fullName, alias);

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryEntity.class, this);
    }
}
