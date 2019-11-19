/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.dto.cache;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DTO for cache sql metadata.
 */
public class CacheSqlMetadata {
    /** Cache name. */
    private String cacheName;

    /** Schema name. */
    private String schemaName;

    /** Table name. */
    private String tblName;

    /** Type name. */
    private String typeName;

    /** Key class. */
    private String keyCls;

    /** Value class. */
    private String valCls;

    /** Fields. */
    private Map<String, String> fields = new LinkedHashMap<>();

    /** Not null fields. */
    private Set<String> notNullFields = new LinkedHashSet<>();

    /** Indexes. */
    private List<CacheSqlIndexMetadata> indexes = new ArrayList<>();

    /**
     * @return Type name.
     */
    public String getTypeName() {
        return typeName;
    }

    /**
     * @param typeName Type name.
     * @return {@code This} for chaining method calls.
     */
    public CacheSqlMetadata setTypeName(String typeName) {
        this.typeName = typeName;

        return this;
    }

    /**
     * @return Cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * @param cacheName Cache name.
     * @return {@code This} for chaining method calls.
     */
    public CacheSqlMetadata setCacheName(String cacheName) {
        this.cacheName = cacheName;

        return this;
    }

    /**
     * @return Schema name.
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * @param schemaName Schema name.
     * @return {@code This} for chaining method calls.
     */
    public CacheSqlMetadata setSchemaName(String schemaName) {
        this.schemaName = schemaName;

        return this;
    }

    /**
     * @return Table name.
     */
    public String getTableName() {
        return tblName;
    }

    /**
     * @param tblName Table name.
     * @return {@code This} for chaining method calls.
     */
    public CacheSqlMetadata setTableName(String tblName) {
        this.tblName = tblName;

        return this;
    }

    /**
     * @return Key class.
     */
    public String getKeyClass() {
        return keyCls;
    }

    /**
     * @param keyCls Key class.
     * @return {@code This} for chaining method calls.
     */
    public CacheSqlMetadata setKeyClass(String keyCls) {
        this.keyCls = keyCls;

        return this;
    }

    /**
     * @return Value class.
     */
    public String getValueClass() {
        return valCls;
    }

    /**
     * @param valCls Value class.
     * @return {@code This} for chaining method calls.
     */
    public CacheSqlMetadata setValueClass(String valCls) {
        this.valCls = valCls;

        return this;
    }

    /**
     * @return Fields.
     */
    public Map<String, String> getFields() {
        return fields;
    }

    /**
     * @param fields Fields.
     * @return {@code This} for chaining method calls.
     */
    public CacheSqlMetadata setFields(Map<String, String> fields) {
        this.fields = fields;

        return this;
    }

    /**
     * @return Not null fields.
     */
    public Set<String> getNotNullFields() {
        return notNullFields;
    }

    /**
     * @param notNullFields Not null fields.
     * @return {@code This} for chaining method calls.
     */
    public CacheSqlMetadata setNotNullFields(Set<String> notNullFields) {
        this.notNullFields = notNullFields;

        return this;
    }

    /**
     * @return Indexes.
     */
    public List<CacheSqlIndexMetadata> getIndexes() {
        return indexes;
    }

    /**
     * @param indexes Indexes.
     * @return {@code This} for chaining method calls.
     */
    public CacheSqlMetadata setIndexes(List<CacheSqlIndexMetadata> indexes) {
        this.indexes = indexes;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheSqlMetadata.class, this);
    }
}
