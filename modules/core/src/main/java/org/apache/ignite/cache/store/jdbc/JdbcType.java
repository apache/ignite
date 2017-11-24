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

package org.apache.ignite.cache.store.jdbc;

import java.io.Serializable;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Description for type that could be stored into database by store.
 */
public class JdbcType implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String cacheName;

    /** Schema name in database. */
    private String dbSchema;

    /** Table name in database. */
    private String dbTbl;

    /** Key class used to store key in cache. */
    private String keyType;

    /** List of fields descriptors for key object. */
    @GridToStringInclude
    private JdbcTypeField[] keyFields;

    /** Value class used to store value in cache. */
    private String valType;

    /** List of fields descriptors for value object. */
    @GridToStringInclude
    private JdbcTypeField[] valFlds;

    /** Custom type hasher. */
    private JdbcTypeHasher hasher;

    /**
     * Empty constructor (all values are initialized to their defaults).
     */
    public JdbcType() {
        /* No-op. */
    }

    /**
     * Copy constructor.
     *
     * @param type Type to copy.
     */
    public JdbcType(JdbcType type) {
        cacheName = type.getCacheName();

        dbSchema = type.getDatabaseSchema();
        dbTbl = type.getDatabaseTable();

        keyType = type.getKeyType();
        keyFields = type.getKeyFields();

        valType = type.getValueType();
        valFlds = type.getValueFields();
    }

    /**
     * Gets associated cache name.
     *
     * @return Cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * Sets associated cache name.
     *
     * @param cacheName Cache name.
     */
    public JdbcType setCacheName(String cacheName) {
        this.cacheName = cacheName;

        return this;
    }

    /**
     * Gets database schema name.
     *
     * @return Schema name.
     */
    public String getDatabaseSchema() {
        return dbSchema;
    }

    /**
     * Sets database schema name.
     *
     * @param dbSchema Schema name.
     */
    public JdbcType setDatabaseSchema(String dbSchema) {
        this.dbSchema = dbSchema;

        return this;
    }

    /**
     * Gets table name in database.
     *
     * @return Table name in database.
     */
    public String getDatabaseTable() {
        return dbTbl;
    }

    /**
     * Table name in database.
     *
     * @param dbTbl Table name in database.
     * @return {@code this} for chaining.
     */
    public JdbcType setDatabaseTable(String dbTbl) {
        this.dbTbl = dbTbl;

        return this;
    }

    /**
     * Gets key type.
     *
     * @return Key type.
     */
    public String getKeyType() {
        return keyType;
    }

    /**
     * Sets key type.
     *
     * @param keyType Key type.
     * @return {@code this} for chaining.
     */
    public JdbcType setKeyType(String keyType) {
        this.keyType = keyType;

        return this;
    }

    /**
     * Sets key type.
     *
     * @param cls Key type class.
     * @return {@code this} for chaining.
     */
    public JdbcType setKeyType(Class<?> cls) {
        setKeyType(cls.getName());

        return this;
    }

    /**
     * Gets value type.
     *
     * @return Key type.
     */
    public String getValueType() {
        return valType;
    }

    /**
     * Sets value type.
     *
     * @param valType Value type.
     * @return {@code this} for chaining.
     */
    public JdbcType setValueType(String valType) {
        this.valType = valType;

        return this;
    }

    /**
     * Sets value type.
     *
     * @param cls Value type class.
     * @return {@code this} for chaining.
     */
    public JdbcType setValueType(Class<?> cls) {
        setValueType(cls.getName());

        return this;
    }

    /**
     * Gets optional persistent key fields (needed only if {@link CacheJdbcPojoStore} is used).
     *
     * @return Persistent key fields.
     */
    public JdbcTypeField[] getKeyFields() {
        return keyFields;
    }

    /**
     * Sets optional persistent key fields (needed only if {@link CacheJdbcPojoStore} is used).
     *
     * @param keyFlds Persistent key fields.
     * @return {@code this} for chaining.
     */
    public JdbcType setKeyFields(JdbcTypeField... keyFlds) {
        this.keyFields = keyFlds;

        return this;
    }

    /**
     * Gets optional persistent value fields (needed only if {@link CacheJdbcPojoStore} is used).
     *
     * @return Persistent value fields.
     */
    public JdbcTypeField[] getValueFields() {
        return valFlds;
    }

    /**
     * Sets optional persistent value fields (needed only if {@link CacheJdbcPojoStore} is used).
     *
     * @param valFlds Persistent value fields.
     * @return {@code this} for chaining.
     */
    public JdbcType setValueFields(JdbcTypeField... valFlds) {
        this.valFlds = valFlds;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcType.class, this);
    }
}
