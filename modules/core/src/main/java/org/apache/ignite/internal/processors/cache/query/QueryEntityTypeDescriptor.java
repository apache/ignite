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

package org.apache.ignite.internal.processors.cache.query;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.cache.CacheException;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Descriptor of type.
 */
public class QueryEntityTypeDescriptor {
    /** Value field names and types with preserved order. */
    @GridToStringInclude
    private final Map<String, Class<?>> fields = new LinkedHashMap<>();

    /** */
    @GridToStringExclude
    private final Map<String, QueryEntityClassProperty> props = new LinkedHashMap<>();

    /** */
    @GridToStringInclude
    private final Set<String> keyProps = new HashSet<>();

    /** */
    @GridToStringInclude
    private final Map<String, QueryEntityIndexDescriptor> indexes = new HashMap<>();

    /** */
    private Set<String> notNullFields = new HashSet<>();

    /** Precision information. */
    private Map<String, Integer> fieldsPrecision = new HashMap<>();

    /** Scale information. */
    private Map<String, Integer> fieldsScale = new HashMap<>();

    /** */
    private QueryEntityIndexDescriptor fullTextIdx;

    /** */
    private Class<?> keyCls;

    /** */
    private Class<?> valCls;

    /** */
    private boolean valTextIdx;

    /**
     * @return Indexes.
     */
    public Map<String, GridQueryIndexDescriptor> indexes() {
        return Collections.<String, GridQueryIndexDescriptor>unmodifiableMap(indexes);
    }

    /**
     * Adds index.
     *
     * @param idxName Index name.
     * @param type Index type.
     * @param inlineSize Inline size.
     * @return Index descriptor.
     */
    public QueryEntityIndexDescriptor addIndex(String idxName, QueryIndexType type, int inlineSize) {
        if (inlineSize < 0 && inlineSize != QueryIndex.DFLT_INLINE_SIZE)
            throw new CacheException("Illegal inline size [idxName=" + idxName + ", inlineSize=" + inlineSize + ']');

        QueryEntityIndexDescriptor idx = new QueryEntityIndexDescriptor(type, inlineSize);

        if (indexes.put(idxName, idx) != null)
            throw new CacheException("Index with name '" + idxName + "' already exists.");

        return idx;
    }

    /**
     * Adds field to index.
     *
     * @param idxName Index name.
     * @param field Field name.
     * @param orderNum Fields order number in index.
     * @param descending Sorting order.
     */
    public void addFieldToIndex(String idxName, String field, int orderNum,
        boolean descending) {
        QueryEntityIndexDescriptor desc = indexes.get(idxName);

        if (desc == null)
            desc = addIndex(idxName, QueryIndexType.SORTED, QueryIndex.DFLT_INLINE_SIZE);

        desc.addField(field, orderNum, descending);
    }

    /**
     * Adds field to text index.
     *
     * @param field Field name.
     */
    public void addFieldToTextIndex(String field) {
        if (fullTextIdx == null) {
            fullTextIdx = new QueryEntityIndexDescriptor(QueryIndexType.FULLTEXT);

            indexes.put(null, fullTextIdx);
        }

        fullTextIdx.addField(field, 0, false);
    }

    /**
     * @return Value class.
     */
    public Class<?> valueClass() {
        return valCls;
    }

    /**
     * Sets value class.
     *
     * @param valCls Value class.
     */
    public void valueClass(Class<?> valCls) {
        this.valCls = valCls;
    }

    /**
     * @return Key class.
     */
    public Class<?> keyClass() {
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

    /**
     * Adds property to the type descriptor.
     *
     * @param prop Property.
     * @param key Property ownership flag (key or not).
     * @param failOnDuplicate Fail on duplicate flag.
     */
    public void addProperty(QueryEntityClassProperty prop, boolean key, boolean failOnDuplicate) {
        String name = prop.fullName();

        if (props.put(name, prop) != null && failOnDuplicate)
            throw new CacheException("Property with name '" + name + "' already exists.");

        fields.put(name, prop.type());

        if (key)
            keyProps.add(name);
    }

    /**
     * Adds a notNull field.
     *
     * @param field notNull field.
     */
    public void addNotNullField(String field) {
        notNullFields.add(field);
    }

    /**
     * Adds fieldsPrecision info.
     *
     * @param field Field.
     * @param precision Precision.
     */
    public void addPrecision(String field, Integer precision) {
        fieldsPrecision.put(field, precision);
    }

    /**
     * Adds fieldsScale info.
     *
     * @param field Field.
     * @param scale Scale.
     */
    public void addScale(String field, int scale) {
        fieldsScale.put(field, scale);
    }

    /**
     * @return notNull fields.
     */
    public Set<String> notNullFields() {
        return notNullFields;
    }

    /**
     * @return Precision info for fields.
     */
    public Map<String, Integer> fieldsPrecision() {
        return fieldsPrecision;
    }

    /**
     * @return Scale info for fields.
     */
    public Map<String, Integer> fieldsScale() {
        return fieldsScale;
    }

    /**
     * @return Class properties.
     */
    public Map<String, QueryEntityClassProperty> properties() {
        return props;
    }

    /**
     * @return Properties keys.
     */
    public Set<String> keyProperties() {
        return keyProps;
    }

    /**
     * @return {@code true} If we need to have a fulltext index on value.
     */
    public boolean valueTextIndex() {
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
    @Override public String toString() {
        return S.toString(QueryEntityTypeDescriptor.class, this);
    }
}

