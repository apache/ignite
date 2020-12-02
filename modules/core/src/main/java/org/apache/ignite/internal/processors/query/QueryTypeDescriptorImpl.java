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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.KEY_SCALE_OUT_OF_RANGE;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.NULL_KEY;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.NULL_VALUE;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.TOO_LONG_KEY;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.TOO_LONG_VALUE;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.VALUE_SCALE_OUT_OF_RANGE;
import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VAL_FIELD_NAME;

/**
 * Descriptor of type.
 */
public class QueryTypeDescriptorImpl implements GridQueryTypeDescriptor {
    /** Cache name. */
    private final String cacheName;

    /** */
    private String name;

    /** Schema name. */
    private String schemaName;

    /** */
    private String tblName;

    /** Value field names and types with preserved order. */
    @GridToStringInclude
    private final LinkedHashMap<String, Class<?>> fields = new LinkedHashMap<>();

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
    private boolean customAffKeyMapper;

    /** */
    private String keyFieldName;

    /** */
    private String valFieldName;

    /** Obsolete. */
    private volatile boolean obsolete;

    /** */
    private List<GridQueryProperty> validateProps;

    /** */
    private List<GridQueryProperty> propsWithDefaultValue;

    /** */
    private final CacheObjectContext coCtx;

    /** Primary key fields. */
    private Set<String> pkFields;

    /**
     * Constructor.
     *
     * @param cacheName Cache name.
     * @param coCtx Cache object context.
     */
    public QueryTypeDescriptorImpl(String cacheName, CacheObjectContext coCtx) {
        this.cacheName = cacheName;
        this.coCtx = coCtx;
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

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return schemaName;
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
    @Override public LinkedHashMap<String, Class<?>> fields() {
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
    @Override public <T> T value(String field, Object key, Object val) throws IgniteCheckedException {
        assert field != null;

        GridQueryProperty prop = property(field);

        if (prop == null)
            throw new IgniteCheckedException("Failed to find field '" + field + "' in type '" + name + "'.");

        return (T)prop.value(key, val);
    }

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
    @Override public boolean matchType(CacheObject val) {
        if (val instanceof BinaryObject)
            return ((BinaryObject)val).type().typeId() == typeId;

        // Value type name can be manually set in QueryEntity to any random value,
        // also for some reason our conversion from setIndexedTypes sets a full class name
        // instead of a simple name there, thus we can have a typeId mismatch.
        // Also, if the type is not in binary format, we always must have it's class available.
        return val.value(coCtx, false).getClass() == valCls;
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
        addProperty(prop, failOnDuplicate, true);
    }

    /**
     * Adds property to the type descriptor.
     *
     * @param prop Property.
     * @param failOnDuplicate Fail on duplicate flag.
     * @param isField {@code True} if {@code prop} if field, {@code False} if prop is "_KEY" or "_VAL".
     * @throws IgniteCheckedException In case of error.
     */
    public void addProperty(GridQueryProperty prop, boolean failOnDuplicate, boolean isField)
        throws IgniteCheckedException {
        String name = prop.name();

        if (props.put(name, prop) != null && failOnDuplicate)
            throw new IgniteCheckedException("Property with name '" + name + "' already exists.");

        if (uppercaseProps.put(name.toUpperCase(), prop) != null && failOnDuplicate)
            throw new IgniteCheckedException("Property with upper cased name '" + name + "' already exists.");

        if (prop.notNull() || prop.precision() != -1 ||
            coCtx.kernalContext().config().getSqlConfiguration().isValidationEnabled()) {
            if (validateProps == null)
                validateProps = new ArrayList<>();

            validateProps.add(prop);
        }

        if (prop.defaultValue() != null) {
            if (propsWithDefaultValue == null)
                propsWithDefaultValue = new ArrayList<>();

            propsWithDefaultValue.add(prop);
        }

        if (isField)
            fields.put(name, prop.type());
    }

    /**
     * Removes a property with specified name.
     *
     * @param name Name of a property to remove.
     */
    public void removeProperty(String name) throws IgniteCheckedException {
        GridQueryProperty prop = props.remove(name);

        if (prop == null)
            throw new IgniteCheckedException("Property with name '" + name + "' does not exist.");

        if (validateProps != null)
            validateProps.remove(prop);

        uppercaseProps.remove(name.toUpperCase());

        fields.remove(name);
    }

    /**
     * @param schemaName Schema name.
     */
    public void schemaName(String schemaName) {
        this.schemaName = schemaName;
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
    @Override public boolean customAffinityKeyMapper() {
        return customAffKeyMapper;
    }

    /**
     * @param customAffKeyMapper Whether custom affinity key mapper is set.
     */
    public void customAffinityKeyMapper(boolean customAffKeyMapper) {
        this.customAffKeyMapper = customAffKeyMapper;
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
    void keyFieldName(String keyFieldName) {
        this.keyFieldName = keyFieldName;
    }

    /** {@inheritDoc} */
    @Override public String keyFieldName() {
        return keyFieldName;
    }

    /**
     * Sets value field name.
     * @param valFieldName value field name.
     */
    void valueFieldName(String valFieldName) {
        this.valFieldName = valFieldName;
    }

    /** {@inheritDoc} */
    @Override public String valueFieldName() {
        return valFieldName;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String keyFieldAlias() {
        return keyFieldName != null ? aliases.get(keyFieldName) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String valueFieldAlias() {
        return valFieldName != null ? aliases.get(valFieldName) : null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override public void validateKeyAndValue(Object key, Object val) throws IgniteCheckedException {
        if (F.isEmpty(validateProps) && F.isEmpty(idxs))
            return;

        validateProps(key, val);

        validateIndexes(key, val);
    }

    /** Validate properties. */
    private void validateProps(Object key, Object val) throws IgniteCheckedException {
        if (F.isEmpty(validateProps))
            return;

        final boolean validateTypes = coCtx.kernalContext().config().getSqlConfiguration().isValidationEnabled();

        for (int i = 0; i < validateProps.size(); ++i) {
            GridQueryProperty prop = validateProps.get(i);

            Object propVal;

            boolean isKey = false;

            if (F.eq(prop.name(), keyFieldAlias()) || (keyFieldName == null && F.eq(prop.name(), KEY_FIELD_NAME))) {
                propVal = key instanceof KeyCacheObject ? ((CacheObject) key).value(coCtx, true) : key;

                isKey = true;
            }
            else if (F.eq(prop.name(), valueFieldAlias()) ||
                (valFieldName == null && F.eq(prop.name(), VAL_FIELD_NAME)))
                propVal = val instanceof CacheObject ? ((CacheObject)val).value(coCtx, true) : val;
            else
                propVal = prop.value(key, val);

            if (propVal == null && prop.notNull()) {
                throw new IgniteSQLException("Null value is not allowed for column '" + prop.name() + "'",
                    isKey ? NULL_KEY : NULL_VALUE);
            }

            if (validateTypes && propVal != null) {
                if (!(propVal instanceof BinaryObject)) {
                    if (!U.box(prop.type()).isAssignableFrom(U.box(propVal.getClass()))) {
                        // Some reference type arrays end up being converted to Object[]
                        if (!(prop.type().isArray() && Object[].class == propVal.getClass() &&
                            Arrays.stream((Object[]) propVal).
                            noneMatch(x -> x != null && !U.box(prop.type().getComponentType()).isAssignableFrom(U.box(x.getClass())))))
                        {
                            throw new IgniteSQLException("Type for a column '" + prop.name() +
                                "' is not compatible with table definition. Expected '" +
                                prop.type().getSimpleName() + "', actual type '" +
                                propVal.getClass().getSimpleName() + "'");
                        }
                    }
                }
                else if (coCtx.kernalContext().cacheObjects().typeId(prop.type().getName()) !=
                        ((BinaryObject)propVal).type().typeId()) {
                    throw new IgniteSQLException("Type for a column '" + prop.name() +
                        "' is not compatible with table definition. Expected '" +
                        prop.type().getSimpleName() + "', actual type '" +
                        ((BinaryObject)propVal).type().typeName() + "'");
                }
            }

            if (propVal == null || prop.precision() == -1)
                continue;

            if (String.class == propVal.getClass() &&
                ((String)propVal).length() > prop.precision()) {
                throw new IgniteSQLException("Value for a column '" + prop.name() + "' is too long. " + 
                    "Maximum length: " + prop.precision() + ", actual length: " + ((CharSequence)propVal).length(),
                    isKey ? TOO_LONG_KEY : TOO_LONG_VALUE);
            }
            else if (BigDecimal.class == propVal.getClass()) {
                BigDecimal dec = (BigDecimal)propVal;

                if (dec.precision() > prop.precision()) {
                    throw new IgniteSQLException("Value for a column '" + prop.name() + "' is out of range. " +
                        "Maximum precision: " + prop.precision() + ", actual precision: " + dec.precision(),
                        isKey ? TOO_LONG_KEY : TOO_LONG_VALUE);
                }
                else if (prop.scale() != -1 &&
                    dec.scale() > prop.scale()) {
                    throw new IgniteSQLException("Value for a column '" + prop.name() + "' is out of range. " +
                        "Maximum scale : " + prop.scale() + ", actual scale: " + dec.scale(),
                        isKey ? KEY_SCALE_OUT_OF_RANGE : VALUE_SCALE_OUT_OF_RANGE);
                }
            }
        }
    }

    /** Validate indexed values. */
    private void validateIndexes(Object key, Object val) throws IgniteCheckedException {
        if (F.isEmpty(idxs))
            return;

        for (QueryIndexDescriptorImpl idx : idxs.values()) {
            for (String idxField : idx.fields()) {
                GridQueryProperty prop = props.get(idxField);

                Object propVal;
                Class<?> propType;

                if (F.eq(idxField, keyFieldAlias()) || F.eq(idxField, KEY_FIELD_NAME)) {
                    propVal = key instanceof KeyCacheObject ? ((CacheObject) key).value(coCtx, true) : key;

                    propType = propVal == null ? null : propVal.getClass();
                }
                else if (F.eq(idxField, valueFieldAlias()) || F.eq(idxField, VAL_FIELD_NAME)) {
                    propVal = val instanceof CacheObject ? ((CacheObject)val).value(coCtx, true) : val;

                    propType = propVal == null ? null : propVal.getClass();
                }
                else {
                    propVal = prop.value(key, val);

                    propType = prop.type();
                }

                if (propVal == null)
                    continue;

                if (!(propVal instanceof BinaryObject)) {
                    if (!U.box(propType).isAssignableFrom(U.box(propVal.getClass()))) {
                        // Some reference type arrays end up being converted to Object[]
                        if (!(propType.isArray() && Object[].class == propVal.getClass() &&
                            Arrays.stream((Object[]) propVal).
                                noneMatch(x -> x != null && !U.box(propType.getComponentType()).isAssignableFrom(U.box(x.getClass())))))
                        {
                            throw new IgniteSQLException("Type for a column '" + idxField +
                                "' is not compatible with index definition. Expected '" +
                                propType.getSimpleName() + "', actual type '" +
                                propVal.getClass().getSimpleName() + "'");
                        }
                    }
                }
                else if (coCtx.kernalContext().cacheObjects().typeId(propType.getName()) !=
                    ((BinaryObject)propVal).type().typeId()) {
                    throw new IgniteSQLException("Type for a column '" + idxField +
                        "' is not compatible with index definition. Expected '" +
                        propType.getSimpleName() + "', actual type '" +
                        ((BinaryObject)propVal).type().typeName() + "'");
                }
            }
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override public void setDefaults(Object key, Object val) throws IgniteCheckedException {
        if (F.isEmpty(propsWithDefaultValue))
            return;

        for (int i = 0; i < propsWithDefaultValue.size(); ++i) {
            GridQueryProperty prop = propsWithDefaultValue.get(i);

            prop.setValue(key, val, prop.defaultValue());
        }
    }

    /** {@inheritDoc} */
    @Override public Set<String> primaryKeyFields() {
        return pkFields == null ? Collections.emptySet() : pkFields;
    }

    /** {@inheritDoc} */
    @Override public void primaryKeyFields(Set<String> keys) {
        pkFields = keys;
    }
}
