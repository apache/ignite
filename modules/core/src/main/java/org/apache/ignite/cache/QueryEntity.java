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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;

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

    /** Fields available for query. A map from field name to type name. */
    private LinkedHashMap<String, String> fields = new LinkedHashMap<>();

    /** Set of field names that belong to the key. */
    private Set<String> keyFields;

    /** Aliases. */
    private Map<String, String> aliases = new HashMap<>();

    /** Collection of query indexes. */
    private Map<String, QueryIndex> idxs = new HashMap<>();

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

        fields = new LinkedHashMap<>(other.fields);
        keyFields = other.keyFields != null ? new HashSet<>(other.keyFields) : null;

        aliases = new HashMap<>(other.aliases);
        idxs = new HashMap<>(other.idxs);

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
     * Gets a collection of index entities.
     *
     * @return Collection of index entities.
     */
    public Collection<QueryIndex> getIndexes() {
        return idxs.values();
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
        for (QueryIndex idx : idxs) {
            if (!F.isEmpty(idx.getFields())) {
                if (idx.getName() == null)
                    idx.setName(QueryUtils.indexName(this, idx));

                if (idx.getIndexType() == null)
                    throw new IllegalArgumentException("Index type is not set " + idx.getName());

                if (!this.idxs.containsKey(idx.getName()))
                    this.idxs.put(idx.getName(), idx);
                else
                    throw new IllegalArgumentException("Duplicate index name: " + idx.getName());
            }
        }

        return this;
    }

    /**
     * Clear indexes.
     */
    public void clearIndexes() {
        this.idxs.clear();
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
}
