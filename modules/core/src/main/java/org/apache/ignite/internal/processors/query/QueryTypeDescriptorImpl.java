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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Descriptor of type.
 */
public class QueryTypeDescriptorImpl implements GridQueryTypeDescriptor {
    /** Cache name. */
    private final String cacheName;

    /** */
    private String name;

    /** */
    private String tblName;

    /** Value field names and types with preserved order. */
    @GridToStringInclude
    private final Map<String, Class<?>> fields = new LinkedHashMap<>();

    /** */
    @GridToStringExclude
    private final Map<String, GridQueryProperty> props = new HashMap<>();

    /** Map with upper cased property names to help find properties based on SQL INSERT and MERGE queries. */
    private final Map<String, GridQueryProperty> uppercaseProps = new HashMap<>();

    /** Mutex for operations on indexes. */
    private final Object idxMux = new Object();

    /** */
    @GridToStringInclude
    private final Map<String, QueryIndexDescriptorImpl> idxs = new HashMap<>();

    /** Aliases. */
    private Map<String, String> aliases;

    /** */
    private QueryIndexDescriptorImpl fullTextIdx;

    /** */
    private Class<?> keyCls;

    /** */
    private Class<?> valCls;

    /** */
    private String keyTypeName;

    /** */
    private String valTypeName;

    /** */
    private boolean valTextIdx;

    /** */
    private int typeId;

    /** */
    private String affKey;

    /** */
    private String keyFieldName;

    /** */
    private String valFieldName;

    /** Obsolete. */
    private volatile boolean obsolete;

    /**
     * Constructor.
     *
     * @param cacheName Cache name.
     */
    public QueryTypeDescriptorImpl(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /**
     * Sets type name.
     *
     * @param name Name.
     */
    public void name(String name) {
        this.name = name;
    }

    /**
     * Gets table name for type.
     * @return Table name.
     */
    @Override public String tableName() {
        return tblName != null ? tblName : name;
    }

    /**
     * Sets table name for type.
     *
     * @param tblName Table name.
     */
    public void tableName(String tblName) {
        this.tblName = tblName;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Class<?>> fields() {
        return fields;
    }

    /** {@inheritDoc} */
    @Override public GridQueryProperty property(String name) {
        GridQueryProperty res = props.get(name);

        if (res == null)
            res = uppercaseProps.get(name.toUpperCase());

        return res;
    }

    /**
     * @return Properties.
     */
    public Map<String, GridQueryProperty> properties() {
        return props;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T value(String field, Object key, Object val) throws IgniteCheckedException {
        assert field != null;

        GridQueryProperty prop = property(field);

        if (prop == null)
            throw new IgniteCheckedException("Failed to find field '" + field + "' in type '" + name + "'.");

        return (T)prop.value(key, val);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void setValue(String field, Object key, Object val, Object propVal)
        throws IgniteCheckedException {
        assert field != null;

        GridQueryProperty prop = property(field);

        if (prop == null)
            throw new IgniteCheckedException("Failed to find field '" + field + "' in type '" + name + "'.");

        prop.setValue(key, val, propVal);
    }

    /** {@inheritDoc} */
    @Override public Map<String, GridQueryIndexDescriptor> indexes() {
        synchronized (idxMux) {
            return Collections.<String, GridQueryIndexDescriptor>unmodifiableMap(idxs);
        }
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        return typeId;
    }

    /**
     * @param typeId Type ID.
     */
    public void typeId(int typeId) {
        this.typeId = typeId;
    }

    /**
     * Get index by name.
     *
     * @param name Name.
     * @return Index.
     */
    @Nullable public QueryIndexDescriptorImpl index(String name) {
        synchronized (idxMux) {
            return idxs.get(name);
        }
    }

    /**
     * @return Raw index descriptors.
     */
    public Collection<QueryIndexDescriptorImpl> indexes0() {
        synchronized (idxMux) {
            return new ArrayList<>(idxs.values());
        }
    }

    /** {@inheritDoc} */
    @Override public GridQueryIndexDescriptor textIndex() {
        return fullTextIdx;
    }

    /**
     * Add index.
     *
     * @param idx Index.
     * @throws IgniteCheckedException If failed.
     */
    public void addIndex(QueryIndexDescriptorImpl idx) throws IgniteCheckedException {
        synchronized (idxMux) {
            if (idxs.put(idx.name(), idx) != null)
                throw new IgniteCheckedException("Index with name '" + idx.name() + "' already exists.");
        }
    }

    /**
     * Drop index.
     *
     * @param idxName Index name.
     */
    public void dropIndex(String idxName) {
        synchronized (idxMux) {
            idxs.remove(idxName);
        }
    }

    /**
     * Chedk if particular field exists.
     *
     * @param field Field.
     * @return {@code True} if exists.
     */
    public boolean hasField(String field) {
        return props.containsKey(field) || QueryUtils.VAL_FIELD_NAME.equalsIgnoreCase(field);
    }

    /**
     * Adds field to text index.
     *
     * @param field Field name.
     * @throws IgniteCheckedException If failed.
     */
    public void addFieldToTextIndex(String field) throws IgniteCheckedException {
        if (fullTextIdx == null)
            fullTextIdx = new QueryIndexDescriptorImpl(this, null, QueryIndexType.FULLTEXT, 0);

        fullTextIdx.addField(field, 0, false);
    }

    /** {@inheritDoc} */
    @Override public Class<?> valueClass() {
        return valCls;
    }

    /**
     * Sets value class.
     *
     * @param valCls Value class.
     */
    public void valueClass(Class<?> valCls) {
        A.notNull(valCls, "Value class must not be null");
        this.valCls = valCls;
    }

    /** {@inheritDoc} */
    @Override public Class<?> keyClass() {
        return keyCls;
    }

    /**
     * Set key class.
     *
     * @param keyCls Key class.
     */
    public void keyClass(Class<?> keyCls) {
        this.keyCls = keyCls;
    }

    /** {@inheritDoc} */
    @Override public String keyTypeName() {
        return keyTypeName;
    }

    /**
     * Set key type name.
     *
     * @param keyTypeName Key type name.
     */
    public void keyTypeName(String keyTypeName) {
        this.keyTypeName = keyTypeName;
    }

    /** {@inheritDoc} */
    @Override public String valueTypeName() {
        return valTypeName;
    }

    /**
     * Set value type name.
     *
     * @param valTypeName Value type name.
     */
    public void valueTypeName(String valTypeName) {
        this.valTypeName = valTypeName;
    }

    /**
     * Adds property to the type descriptor.
     *
     * @param prop Property.
     * @param failOnDuplicate Fail on duplicate flag.
     * @throws IgniteCheckedException In case of error.
     */
    public void addProperty(GridQueryProperty prop, boolean failOnDuplicate) throws IgniteCheckedException {
        String name = prop.name();

        if (props.put(name, prop) != null && failOnDuplicate)
            throw new IgniteCheckedException("Property with name '" + name + "' already exists.");

        if (uppercaseProps.put(name.toUpperCase(), prop) != null && failOnDuplicate)
            throw new IgniteCheckedException("Property with upper cased name '" + name + "' already exists.");

        fields.put(name, prop.type());
    }

    /** {@inheritDoc} */
    @Override public boolean valueTextIndex() {
        return valTextIdx;
    }

    /**
     * Sets if this value should be text indexed.
     *
     * @param valTextIdx Flag value.
     */
    public void valueTextIndex(boolean valTextIdx) {
        this.valTextIdx = valTextIdx;
    }

    /** {@inheritDoc} */
    @Override public String affinityKey() {
        return affKey;
    }

    /**
     * @param affKey Affinity key field.
     */
    public void affinityKey(String affKey) {
        this.affKey = affKey;
    }

    /**
     * @return Aliases.
     */
    public Map<String, String> aliases() {
        return aliases != null ? aliases : Collections.<String, String>emptyMap();
    }

    /**
     * @param aliases Aliases.
     */
    public void aliases(Map<String, String> aliases) {
        this.aliases = aliases;
    }

    /**
     * @return {@code True} if obsolete.
     */
    public boolean obsolete() {
        return obsolete;
    }

    /**
     * Mark index as obsolete.
     */
    public void markObsolete() {
        obsolete = true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryTypeDescriptorImpl.class, this);
    }

    /**
     * Sets key field name.
     * @param keyFieldName Key field name.
     */
    public void keyFieldName(String keyFieldName) {
        this.keyFieldName = keyFieldName;
    }

    /** {@inheritDoc} */
    @Override public String keyFieldName() {
        return keyFieldName;
    }

    /**
     * Sets value field name.
     * @param valueFieldName value field name.
     */
    public void valueFieldName(String valueFieldName) {
        this.valFieldName = valueFieldName;
    }

    /** {@inheritDoc} */
    @Override public String valueFieldName() {
        return valFieldName;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String keyFieldAlias() {
        return keyFieldName != null ? aliases.get(keyFieldName) : null;
    }

    @Nullable @Override public String valueFieldAlias() {
        return valFieldName != null ? aliases.get(valFieldName) : null;
    }
}
