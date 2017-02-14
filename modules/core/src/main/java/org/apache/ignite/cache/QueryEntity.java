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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
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
     */
    public void setKeyType(String keyType) {
        this.keyType = keyType;
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
     */
    public void setValueType(String valType) {
        this.valType = valType;
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
     */
    public void setFields(LinkedHashMap<String, String> fields) {
        this.fields = fields;
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
     */
    public void setKeyFields(Set<String> keyFields) {
        this.keyFields = keyFields;
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
     */
    public void setAliases(Map<String, String> aliases) {
        this.aliases = aliases;
    }

    /**
     * Sets a collection of index entities.
     *
     * @param idxs Collection of index entities.
     */
    public void setIndexes(Collection<QueryIndex> idxs) {
        for (QueryIndex idx : idxs) {
            if (!F.isEmpty(idx.getFields())) {
                if (idx.getName() == null)
                    idx.setName(defaultIndexName(idx));

                if (!this.idxs.containsKey(idx.getName()))
                    this.idxs.put(idx.getName(), idx);
                else
                    throw new IllegalArgumentException("Duplicate index name: " + idx.getName());
            }
        }
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
     */
    public void addQueryField(String fullName, String type, String alias) {
        A.notNull(fullName, "fullName");
        A.notNull(type, "type");

        fields.put(fullName, type);

        if (alias != null)
            aliases.put(fullName, alias);
    }

    /**
     * Ensures that index with the given name exists.
     *
     * @param idxName Index name.
     * @param idxType Index type.
     */
    public void ensureIndex(String idxName, QueryIndexType idxType) {
        QueryIndex idx = idxs.get(idxName);

        if (idx == null) {
            idx = new QueryIndex();

            idx.setName(idxName);
            idx.setIndexType(idxType);

            idxs.put(idxName, idx);
        }
        else
            throw new IllegalArgumentException("An index with the same name and of a different type already exists " +
                "[idxName=" + idxName + ", existingIdxType=" + idx.getIndexType() + ", newIdxType=" + idxType + ']');
    }

    /**
     * Generates default index name by concatenating all index field names.
     *
     * @param idx Index to build name for.
     * @return Index name.
     */
    public static String defaultIndexName(QueryIndex idx) {
        StringBuilder idxName = new StringBuilder();

        for (Map.Entry<String, Boolean> field : idx.getFields().entrySet()) {
            idxName.append(field.getKey());

            idxName.append('_');
            idxName.append(field.getValue() ? "asc_" : "desc_");
        }

        for (int i = 0; i < idxName.length(); i++) {
            char ch = idxName.charAt(i);

            if (Character.isWhitespace(ch))
                idxName.setCharAt(i, '_');
            else
                idxName.setCharAt(i, Character.toLowerCase(ch));
        }

        idxName.append("idx");

        return idxName.toString();
    }
}
