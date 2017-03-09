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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Descriptor of type.
 */
public class QueryTypeDescriptorImpl implements GridQueryTypeDescriptor {
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

    /** */
    @GridToStringInclude
    private final Map<String, QueryIndexDescriptorImpl> indexes = new HashMap<>();

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
    private String affKey;

    /** SPI can decide not to register this type. */
    private boolean registered;

    /**
     * @return {@code True} if type registration in SPI was finished and type was not rejected.
     */
    public boolean registered() {
        return registered;
    }

    /**
     * @param registered Sets registered flag.
     */
    public void registered(boolean registered) {
        this.registered = registered;
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
    public String tableName() {
        return tblName;
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
        return Collections.<String, GridQueryIndexDescriptor>unmodifiableMap(indexes);
    }

    /**
     * Adds index.
     *
     * @param idxName Index name.
     * @param type Index type.
     * @return Index descriptor.
     * @throws IgniteCheckedException In case of error.
     */
    public QueryIndexDescriptorImpl addIndex(String idxName, QueryIndexType type) throws IgniteCheckedException {
        QueryIndexDescriptorImpl idx = new QueryIndexDescriptorImpl(type);

        if (indexes.put(idxName, idx) != null)
            throw new IgniteCheckedException("Index with name '" + idxName + "' already exists.");

        return idx;
    }

    /**
     * Adds field to index.
     *
     * @param idxName Index name.
     * @param field Field name.
     * @param orderNum Fields order number in index.
     * @param descending Sorting order.
     * @throws IgniteCheckedException If failed.
     */
    public void addFieldToIndex(String idxName, String field, int orderNum,
        boolean descending) throws IgniteCheckedException {
        QueryIndexDescriptorImpl desc = indexes.get(idxName);

        if (desc == null)
            desc = addIndex(idxName, QueryIndexType.SORTED);

        desc.addField(field, orderNum, descending);
    }

    /**
     * Adds field to text index.
     *
     * @param field Field name.
     */
    public void addFieldToTextIndex(String field) {
        if (fullTextIdx == null) {
            fullTextIdx = new QueryIndexDescriptorImpl(QueryIndexType.FULLTEXT);

            indexes.put(null, fullTextIdx);
        }

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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryTypeDescriptorImpl.class, this);
    }
}
