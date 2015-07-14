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

import org.apache.ignite.cache.store.jdbc.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;

import java.io.*;
import java.util.*;

/**
 * Cache type metadata need for configuration of indexes or automatic persistence.
 */
public class CacheTypeMetadata implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Schema name in database. */
    private String dbSchema;

    /** Table name in database. */
    private String dbTbl;

    /** Key class used to store key in cache. */
    private String keyType;

    /** Value class used to store value in cache. */
    private String valType;

    /** Optional persistent key fields (needed only if {@link CacheJdbcPojoStore} is used). */
    @GridToStringInclude
    private Collection<CacheTypeFieldMetadata> keyFields;

    /** Optional persistent value fields (needed only if {@link CacheJdbcPojoStore} is used). */
    @GridToStringInclude
    private Collection<CacheTypeFieldMetadata> valFields;

    /** Field name-to-type map to be queried, in addition to indexed fields. */
    @GridToStringInclude
    private Map<String, Class<?>> qryFlds;

    /** Field name-to-type map to index in ascending order. */
    @GridToStringInclude
    private Map<String, Class<?>> ascFlds;

    /** Field name-to-type map to index in descending order. */
    @GridToStringInclude
    private Map<String, Class<?>> descFlds;

    /** Fields to index as text. */
    @GridToStringInclude
    private Collection<String> txtFlds;

    /** Fields to create group indexes for. */
    @GridToStringInclude
    private Map<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> grps;

    /**
     * Default constructor.
     */
    public CacheTypeMetadata() {
        keyFields = new ArrayList<>();

        valFields = new ArrayList<>();

        qryFlds = new LinkedHashMap<>();

        ascFlds = new LinkedHashMap<>();

        descFlds = new LinkedHashMap<>();

        txtFlds = new LinkedHashSet<>();

        grps = new LinkedHashMap<>();
    }

    /**
     * Copy constructor.
     */
    public CacheTypeMetadata(CacheTypeMetadata src) {
        dbSchema = src.getDatabaseSchema();

        dbTbl = src.getDatabaseTable();

        keyType = src.getKeyType();

        valType = src.getValueType();

        keyFields = new ArrayList<>(src.getKeyFields());

        valFields = new ArrayList<>(src.getValueFields());

        qryFlds = new LinkedHashMap<>(src.getQueryFields());

        ascFlds = new LinkedHashMap<>(src.getAscendingFields());

        descFlds = new LinkedHashMap<>(src.getDescendingFields());

        txtFlds = new LinkedHashSet<>(src.getTextFields());

        grps = new LinkedHashMap<>(src.getGroups());
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
    public void setDatabaseSchema(String dbSchema) {
        this.dbSchema = dbSchema;
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
     */
    public void setDatabaseTable(String dbTbl) {
        this.dbTbl = dbTbl;
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
     */
    public void setKeyType(String keyType) {
        this.keyType = keyType;
    }

    /**
     * Sets key type.
     *
     * @param cls Key type class.
     */
    public void setKeyType(Class<?> cls) {
        setKeyType(cls.getName());
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
     */
    public void setValueType(String valType) {
        this.valType = valType;
    }

    /**
     * Sets value type.
     *
     * @param cls Value type class.
     */
    public void setValueType(Class<?> cls) {
        setValueType(cls.getName());
    }

    /**
     * Gets optional persistent key fields (needed only if {@link CacheJdbcPojoStore} is used).
     *
     * @return Persistent key fields.
     */
    public Collection<CacheTypeFieldMetadata> getKeyFields() {
        return keyFields;
    }

    /**
     * Sets optional persistent key fields (needed only if {@link CacheJdbcPojoStore} is used).
     *
     * @param keyFields Persistent key fields.
     */
    public void setKeyFields(Collection<CacheTypeFieldMetadata> keyFields) {
        this.keyFields = keyFields;
    }

    /**
     * Gets optional persistent value fields (needed only if {@link CacheJdbcPojoStore} is used).
     *
     * @return Persistent value fields.
     */
    public Collection<CacheTypeFieldMetadata> getValueFields() {
        return valFields;
    }

    /**
     * Sets optional persistent value fields (needed only if {@link CacheJdbcPojoStore} is used).
     *
     * @param valFields Persistent value fields.
     */
    public void setValueFields(Collection<CacheTypeFieldMetadata> valFields) {
        this.valFields = valFields;
    }

    /**
     * Gets name-to-type map for query-enabled fields.
     *
     * @return Name-to-type map for query-enabled fields.
     */
    public Map<String, Class<?>> getQueryFields() {
        return qryFlds;
    }

    /**
     * Sets name-to-type map for query-enabled fields.
     *
     * @param qryFlds Name-to-type map for query-enabled fields.
     */
    public void setQueryFields(Map<String, Class<?>> qryFlds) {
        this.qryFlds = qryFlds;
    }

    /**
     * Gets name-to-type map for ascending-indexed fields.
     *
     * @return Name-to-type map for ascending-indexed fields.
     */
    public Map<String, Class<?>> getAscendingFields() {
        return ascFlds;
    }

    /**
     * Sets name-to-type map for ascending-indexed fields.
     *
     * @param ascFlds Name-to-type map for ascending-indexed fields.
     */
    public void setAscendingFields(Map<String, Class<?>> ascFlds) {
        this.ascFlds = ascFlds;
    }

    /**
     * Gets name-to-type map for descending-indexed fields.
     *
     * @return Name-to-type map of descending-indexed fields.
     */
    public Map<String, Class<?>> getDescendingFields() {
        return descFlds;
    }

    /**
     * Sets name-to-type map for descending-indexed fields.
     *
     * @param descFlds Name-to-type map of descending-indexed fields.
     */
    public void setDescendingFields(Map<String, Class<?>> descFlds) {
        this.descFlds = descFlds;
    }

    /**
     * Gets text-indexed fields.
     *
     * @return Collection of text indexed fields.
     */
    public Collection<String> getTextFields() {
        return txtFlds;
    }

    /**
     * Sets text-indexed fields.
     *
     * @param txtFlds Text-indexed fields.
     */
    public void setTextFields(Collection<String> txtFlds) {
        this.txtFlds = txtFlds;
    }

    /**
     * Gets group-indexed fields.
     *
     * @return Map of group-indexed fields.
     */
    public Map<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> getGroups() {
        return grps;
    }

    /**
     * Sets group-indexed fields.
     *
     * @param grps Map of group-indexed fields from index name to index fields.
     */
    public void setGroups(Map<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> grps) {
        this.grps = grps;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheTypeMetadata.class, this);
    }
}
