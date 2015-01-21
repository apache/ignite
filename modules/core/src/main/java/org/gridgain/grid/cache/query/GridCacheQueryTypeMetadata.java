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

package org.gridgain.grid.cache.query;

import org.apache.ignite.lang.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Cache query type metadata.
 */
public class GridCacheQueryTypeMetadata {
    /** Type name, e.g. class name. */
    private String type;

    /** Schema name in database. */
    private String schema;

    /** Table name in database. */
    private String tbl;

    /** Key class. */
    private String keyType;

    /** Type descriptors. */
    @GridToStringInclude
    private Collection<GridCacheQueryTypeDescriptor> keyDescs;

    /** Type descriptors. */
    @GridToStringInclude
    private Collection<GridCacheQueryTypeDescriptor> valDescs;

    /** Fields to be queried, in addition to indexed fields. */
    @GridToStringInclude
    private Map<String, Class<?>> qryFlds;

    /** Fields to index in ascending order. */
    @GridToStringInclude
    private Map<String, Class<?>> ascFlds;

    /** Fields to index in descending order. */
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
    public GridCacheQueryTypeMetadata() {
        keyDescs = new ArrayList<>();

        valDescs = new ArrayList<>();

        qryFlds = new LinkedHashMap<>();

        ascFlds = new LinkedHashMap<>();

        descFlds = new LinkedHashMap<>();

        txtFlds = new LinkedHashSet<>();

        grps = new LinkedHashMap<>();
    }

    /**
     *
     */
    public GridCacheQueryTypeMetadata(GridCacheQueryTypeMetadata src) {
        type = src.getType();
        keyType = src.getKeyType();

        schema = src.getSchema();
        tbl = src.getTableName();

        keyDescs = new ArrayList<>(src.getKeyDescriptors());
        valDescs = new ArrayList<>(src.getValueDescriptors());

        qryFlds = new LinkedHashMap<>(src.getQueryFields());
        ascFlds = new LinkedHashMap<>(src.getAscendingFields());
        descFlds = new LinkedHashMap<>(src.getDescendingFields());
        txtFlds = new LinkedHashSet<>(src.getTextFields());

        grps = new LinkedHashMap<>(src.getGroups());
    }

    /**
     * Gets type (e.g. class name).
     *
     * @return Type name.
     */
    public String getType() {
        return type;
    }

    /**
     * Sets type.
     *
     * @param cls Type class.
     */
    public void setType(Class<?> cls) {
        setType(cls.getName());
    }

    /**
     * Sets type.
     *
     * @param type Type name.
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Gets database schema name.
     *
     * @return Schema name.
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Sets database schema name.
     *
     * @param schema Schema name.
     */
    public void setSchema(String schema) {
        this.schema = schema;
    }

    /**
     * Gets table name in database.
     *
     * @return Table name in database.
     */
    public String getTableName() {
        return tbl;
    }

    /**
     * Table name in database.
     *
     * @param tbl Table name in database.
     */
    public void setTableName(String tbl) {
        this.tbl = tbl;
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
     * Gets key fields type descriptors.
     *
     * @return Key fields type descriptors.
     */
    public Collection<GridCacheQueryTypeDescriptor> getKeyDescriptors() {
        return keyDescs;
    }

    /**
     * Sets key fields type descriptors.
     *
     * @param keyDescs Key fields type descriptors.
     */
    public void setKeyDescriptors(Collection<GridCacheQueryTypeDescriptor> keyDescs) {
        this.keyDescs = keyDescs;
    }

    /**
     * Gets value fields type descriptors.
     *
     * @return Key value type descriptors.
     */
    public Collection<GridCacheQueryTypeDescriptor> getValueDescriptors() {
        return valDescs;
    }

    /**
     * Sets value fields type descriptors.
     *
     * @param valDescs Value fields type descriptors.
     */
    public void setValueDescriptors(Collection<GridCacheQueryTypeDescriptor> valDescs) {
        this.valDescs = valDescs;
    }

    /**
     * Gets query-enabled fields.
     *
     * @return Collection of fields available for query.
     */
    public Map<String, Class<?>> getQueryFields() {
        return qryFlds;
    }

    /**
     * Sets query fields map.
     *
     * @param qryFlds Query fields.
     */
    public void setQueryFields(Map<String, Class<?>> qryFlds) {
        this.qryFlds = qryFlds;
    }

    /**
     * Gets ascending-indexed fields.
     *
     * @return Map of ascending-indexed fields.
     */
    public Map<String, Class<?>> getAscendingFields() {
        return ascFlds;
    }

    /**
     * Sets ascending-indexed fields.
     *
     * @param ascFlds Map of ascending-indexed fields.
     */
    public void setAscendingFields(Map<String, Class<?>> ascFlds) {
        this.ascFlds = ascFlds;
    }

    /**
     * Gets descending-indexed fields.
     *
     * @return Map of descending-indexed fields.
     */
    public Map<String, Class<?>> getDescendingFields() {
        return descFlds;
    }

    /**
     * Sets descending-indexed fields.
     *
     * @param descFlds Map of descending-indexed fields.
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
        return S.toString(GridCacheQueryTypeMetadata.class, this);
    }
}
