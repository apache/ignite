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

import javax.cache.CacheException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cache.query.annotations.QueryGroupIndex;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QueryTextField;
import org.apache.ignite.internal.processors.cache.query.QueryEntityClassProperty;
import org.apache.ignite.internal.processors.cache.query.QueryEntityTypeDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAbstractOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAlterTableAddColumnOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaIndexCreateOperation;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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

    /** Key name. Can be used in field list to denote the key as a whole. */
    private String keyFieldName;

    /** Value name. Can be used in field list to denote the entire value. */
    private String valueFieldName;

    /** Fields available for query. A map from field name to type name. */
    @GridToStringInclude
    private LinkedHashMap<String, String> fields = new LinkedHashMap<>();

    /** Set of field names that belong to the key. */
    @GridToStringInclude
    private Set<String> keyFields;

    /** Aliases. */
    @GridToStringInclude
    private Map<String, String> aliases = new HashMap<>();

    /** Collection of query indexes. */
    @GridToStringInclude
    private Collection<QueryIndex> idxs;

    /** Table name. */
    private String tableName;

    /** Fields that must have non-null value. NB: DO NOT remove underscore to avoid clashes with QueryEntityEx. */
    private Set<String> _notNullFields;

    /** Fields default values. */
    private Map<String, Object> defaultFieldValues = new HashMap<>();

    /** Precision(Maximum length) for fields. */
    private Map<String, Integer> fieldsPrecision = new HashMap<>();

    /** Scale for fields. */
    private Map<String, Integer> fieldsScale = new HashMap<>();

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

        keyFieldName = other.keyFieldName;
        valueFieldName = other.valueFieldName;

        fields = new LinkedHashMap<>(other.fields);
        keyFields = other.keyFields != null ? new HashSet<>(other.keyFields) : null;

        aliases = new HashMap<>(other.aliases);
        idxs = other.idxs != null ? new ArrayList<>(other.idxs) : null;

        tableName = other.tableName;

        _notNullFields = other._notNullFields != null ? new HashSet<>(other._notNullFields) : null;

        defaultFieldValues = other.defaultFieldValues != null ? new HashMap<>(other.defaultFieldValues)
            : new HashMap<String, Object>();

        fieldsPrecision = other.fieldsPrecision != null ? new HashMap<>(other.fieldsPrecision) : new HashMap<>();

        fieldsScale = other.fieldsScale != null ? new HashMap<>(other.fieldsScale) : new HashMap<>();
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
     * Creates a query entity with the given key and value types.
     *
     * @param keyCls Key type.
     * @param valCls Value type.
     */
    public QueryEntity(Class<?> keyCls, Class<?> valCls) {
        this(convert(processKeyAndValueClasses(keyCls, valCls)));
    }

    /**
     * Make query entity patch. This patch can only add properties to entity and can't remove them.
     * Other words, the patch will contain only add operations(e.g. add column, create index) and not remove ones.
     *
     * @param target Query entity to which this entity should be expanded.
     * @return Patch which contains operations for expanding this entity.
     */
    @NotNull public QueryEntityPatch makePatch(QueryEntity target) {
        if (target == null)
            return QueryEntityPatch.empty();

        StringBuilder conflicts = new StringBuilder();

        checkEquals(conflicts, "keyType", keyType, target.keyType);
        checkEquals(conflicts, "valType", valType, target.valType);
        checkEquals(conflicts, "keyFieldName", keyFieldName, target.keyFieldName);
        checkEquals(conflicts, "valueFieldName", valueFieldName, target.valueFieldName);
        checkEquals(conflicts, "tableName", tableName, target.tableName);

        List<QueryField> queryFieldsToAdd = checkFields(target, conflicts);

        Collection<QueryIndex> indexesToAdd = checkIndexes(target, conflicts);

        if (conflicts.length() != 0)
            return QueryEntityPatch.conflict(tableName + " conflict: \n" + conflicts.toString());

        Collection<SchemaAbstractOperation> patchOperations = new ArrayList<>();

        if (!queryFieldsToAdd.isEmpty())
            patchOperations.add(new SchemaAlterTableAddColumnOperation(
                UUID.randomUUID(),
                null,
                null,
                tableName,
                queryFieldsToAdd,
                true,
                true
            ));

        if (!indexesToAdd.isEmpty()) {
            for (QueryIndex index : indexesToAdd) {
                patchOperations.add(new SchemaIndexCreateOperation(
                    UUID.randomUUID(),
                    null,
                    null,
                    tableName,
                    index,
                    true,
                    0
                ));
            }
        }

        return QueryEntityPatch.patch(patchOperations);
    }

    /**
     * Comparing local fields and target fields.
     *
     * @param target Query entity for check.
     * @param conflicts Storage of conflicts.
     * @return Indexes which exist in target and not exist in local.
     */
    @NotNull private Collection<QueryIndex> checkIndexes(QueryEntity target, StringBuilder conflicts) {
        HashSet<QueryIndex> indexesToAdd = new HashSet<>();

        Map<String, QueryIndex> currentIndexes = new HashMap<>();

        for (QueryIndex index : getIndexes()) {
            if (currentIndexes.put(index.getName(), index) != null)
                throw new IllegalStateException("Duplicate key");
        }

        for (QueryIndex queryIndex : target.getIndexes()) {
            if(currentIndexes.containsKey(queryIndex.getName())) {
                checkEquals(
                    conflicts,
                    "index " + queryIndex.getName(),
                    currentIndexes.get(queryIndex.getName()),
                    queryIndex
                );
            }
            else
                indexesToAdd.add(queryIndex);
        }
        return indexesToAdd;
    }

    /**
     * Comparing local entity fields and target entity fields.
     *
     * @param target Query entity for check.
     * @param conflicts Storage of conflicts.
     * @return Fields which exist in target and not exist in local.
     */
    private List<QueryField> checkFields(QueryEntity target, StringBuilder conflicts) {
        List<QueryField> queryFieldsToAdd = new ArrayList<>();

        for (Map.Entry<String, String> targetField : target.getFields().entrySet()) {
            String targetFieldName = targetField.getKey();
            String targetFieldType = targetField.getValue();

            if (getFields().containsKey(targetFieldName)) {
                checkEquals(
                    conflicts,
                    "fieldType of " + targetFieldName,
                    getFields().get(targetFieldName),
                    targetFieldType
                );

                checkEquals(
                    conflicts,
                    "nullable of " + targetFieldName,
                    contains(getNotNullFields(), targetFieldName),
                    contains(target.getNotNullFields(), targetFieldName)
                );

                checkEquals(
                    conflicts,
                    "default value of " + targetFieldName,
                    getFromMap(getDefaultFieldValues(), targetFieldName),
                    getFromMap(target.getDefaultFieldValues(), targetFieldName)
                );

                checkEquals(conflicts,
                    "precision of " + targetFieldName,
                    getFromMap(getFieldsPrecision(), targetFieldName),
                    getFromMap(target.getFieldsPrecision(), targetFieldName));

                checkEquals(
                    conflicts,
                    "scale of " + targetFieldName,
                    getFromMap(getFieldsScale(), targetFieldName),
                    getFromMap(target.getFieldsScale(), targetFieldName));
            }
            else {
                Integer precision = getFromMap(target.getFieldsPrecision(), targetFieldName);
                Integer scale = getFromMap(target.getFieldsScale(), targetFieldName);

                queryFieldsToAdd.add(new QueryField(
                    targetFieldName,
                    targetFieldType,
                    !contains(target.getNotNullFields(),targetFieldName),
                    getFromMap(target.getDefaultFieldValues(), targetFieldName),
                    precision == null ? -1 : precision,
                    scale == null ? -1 : scale
                ));
            }
        }

        return queryFieldsToAdd;
    }

    /**
     * @param collection Collection for checking.
     * @param elementToCheck Element for checking to containing in collection.
     * @return {@code true} if collection contain elementToCheck.
     */
    private static boolean contains(Collection<String> collection, String elementToCheck) {
        return collection != null && collection.contains(elementToCheck);
    }

    /**
     * @return Value from sourceMap or null if map is null.
     */
    private static <V> V getFromMap(Map<String, V> sourceMap, String key) {
        return sourceMap == null ? null : sourceMap.get(key);
    }

    /**
     * Comparing two objects and add formatted text to conflicts if needed.
     *
     * @param conflicts Storage of conflicts resulting error message.
     * @param name Name of comparing object.
     * @param local Local object.
     * @param received Received object.
     */
    private <V> void checkEquals(StringBuilder conflicts, String name, V local, V received) {
        if (!Objects.equals(local, received))
            conflicts.append(String.format("%s is different: local=%s, received=%s\n", name, local, received));
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
     * Attempts to get key type from fields in case it was not set directly.
     *
     * @return Key type.
     */
    public String findKeyType() {
        if (keyType != null)
            return keyType;

        if (fields != null && keyFieldName != null)
            return fields.get(keyFieldName);

        return null;
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
     * Attempts to get value type from fields in case it was not set directly.
     *
     * @return Value type.
     */
    public String findValueType() {
        if (valType != null)
            return valType;

        if (fields != null && valueFieldName != null)
            return fields.get(valueFieldName);

        return null;
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
     * Gets key field name.
     *
     * @return Key name.
     */
    public String getKeyFieldName() {
        return keyFieldName;
    }

    /**
     * Sets key field name.
     *
     * @param keyFieldName Key name.
     * @return {@code this} for chaining.
     */
    public QueryEntity setKeyFieldName(String keyFieldName) {
        this.keyFieldName = keyFieldName;

        return this;
    }

    /**
     * Get value field name.
     *
     * @return Value name.
     */
    public String getValueFieldName() {
        return valueFieldName;
    }

    /**
     * Sets value field name.
     *
     * @param valueFieldName value name.
     * @return {@code this} for chaining.
     */
    public QueryEntity setValueFieldName(String valueFieldName) {
        this.valueFieldName = valueFieldName;

        return this;
    }

    /**
     * Gets a collection of index entities.
     *
     * @return Collection of index entities.
     */
    @NotNull public Collection<QueryIndex> getIndexes() {
        return idxs == null ? Collections.<QueryIndex>emptyList() : idxs;
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
        this.idxs = idxs;

        return this;
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
     *
     * @param tableName table name
     * @return {@code this} for chaining.
     */
    public QueryEntity setTableName(String tableName) {
        this.tableName = tableName;

        return this;
    }

    /**
     * Gets names of fields that must be checked for null.
     *
     * @return Set of names of fields that must have non-null values.
     */
    @Nullable public Set<String> getNotNullFields() {
        return _notNullFields;
    }

    /**
     * Sets names of fields that must checked for null.
     *
     * @param notNullFields Set of names of fields that must have non-null values.
     * @return {@code this} for chaining.
     */
    public QueryEntity setNotNullFields(@Nullable Set<String> notNullFields) {
        this._notNullFields = notNullFields;

        return this;
    }

    /**
     * @return Precision map for a fields.
     */
    public Map<String, Integer> getFieldsPrecision() {
        return fieldsPrecision;
    }

    /**
     * Sets fieldsPrecision map for a fields.
     *
     * @param fieldsPrecision Precision map for a fields.
     * @return {@code This} for chaining.
     */
    public QueryEntity setFieldsPrecision(Map<String, Integer> fieldsPrecision) {
        this.fieldsPrecision = fieldsPrecision;

        return this;
    }

    /**
     * @return Scale map for a fields.
     */
    public Map<String, Integer> getFieldsScale() {
        return fieldsScale;
    }

    /**
     * Sets fieldsScale map for a fields.
     *
     * @param fieldsScale Scale map for a fields.
     * @return {@code This} for chaining.
     */
    public QueryEntity setFieldsScale(Map<String, Integer> fieldsScale) {
        this.fieldsScale = fieldsScale;

        return this;
    }

    /**
     * Gets fields default values.
     *
     * @return Field's name to default value map.
     */
    public Map<String, Object> getDefaultFieldValues() {
        return defaultFieldValues;
    }

    /**
     * Sets fields default values.
     *
     * @param defaultFieldValues Field's name to default value map.
     * @return {@code this} for chaining.
     */
    public QueryEntity setDefaultFieldValues(Map<String, Object> defaultFieldValues) {
        this.defaultFieldValues = defaultFieldValues;

        return this;
    }

    /**
     * Utility method for building query entities programmatically.
     *
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

    /**
     * @param desc Type descriptor.
     * @return Type metadata.
     */
    private static QueryEntity convert(QueryEntityTypeDescriptor desc) {
        QueryEntity entity = new QueryEntity();

        // Key and val types.
        entity.setKeyType(desc.keyClass().getName());
        entity.setValueType(desc.valueClass().getName());

        for (QueryEntityClassProperty prop : desc.properties().values())
            entity.addQueryField(prop.fullName(), U.box(prop.type()).getName(), prop.alias());

        entity.setKeyFields(desc.keyProperties());

        QueryIndex txtIdx = null;

        Collection<QueryIndex> idxs = new ArrayList<>();

        for (Map.Entry<String, GridQueryIndexDescriptor> idxEntry : desc.indexes().entrySet()) {
            GridQueryIndexDescriptor idx = idxEntry.getValue();

            if (idx.type() == QueryIndexType.FULLTEXT) {
                assert txtIdx == null;

                txtIdx = new QueryIndex();

                txtIdx.setIndexType(QueryIndexType.FULLTEXT);

                txtIdx.setFieldNames(idx.fields(), true);
                txtIdx.setName(idxEntry.getKey());
            }
            else {
                QueryIndex sortedIdx = new QueryIndex();

                sortedIdx.setIndexType(idx.type());

                LinkedHashMap<String, Boolean> fields = new LinkedHashMap<>();

                for (String f : idx.fields())
                    fields.put(f, !idx.descending(f));

                sortedIdx.setFields(fields);

                sortedIdx.setName(idxEntry.getKey());
                sortedIdx.setInlineSize(idx.inlineSize());

                idxs.add(sortedIdx);
            }
        }

        if (desc.valueTextIndex()) {
            if (txtIdx == null) {
                txtIdx = new QueryIndex();

                txtIdx.setIndexType(QueryIndexType.FULLTEXT);

                txtIdx.setFieldNames(Arrays.asList(QueryUtils.VAL_FIELD_NAME), true);
            }
            else
                txtIdx.getFields().put(QueryUtils.VAL_FIELD_NAME, true);
        }

        if (txtIdx != null)
            idxs.add(txtIdx);

        if (!F.isEmpty(idxs))
            entity.setIndexes(idxs);

        if (!F.isEmpty(desc.notNullFields()))
            entity.setNotNullFields(desc.notNullFields());

        if (!F.isEmpty(desc.fieldsPrecision()))
            entity.setFieldsPrecision(desc.fieldsPrecision());

        if (!F.isEmpty(desc.fieldsScale()))
            entity.setFieldsScale(desc.fieldsScale());

        return entity;
    }

    /**
     * @param keyCls Key class.
     * @param valCls Value class.
     * @return Type descriptor.
     */
    private static QueryEntityTypeDescriptor processKeyAndValueClasses(
        Class<?> keyCls,
        Class<?> valCls
    ) {
        QueryEntityTypeDescriptor d = new QueryEntityTypeDescriptor();

        d.keyClass(keyCls);
        d.valueClass(valCls);

        processAnnotationsInClass(true, d.keyClass(), d, null);
        processAnnotationsInClass(false, d.valueClass(), d, null);

        return d;
    }

    /**
     * Process annotations for class.
     *
     * @param key If given class relates to key.
     * @param cls Class.
     * @param type Type descriptor.
     * @param parent Parent in case of embeddable.
     */
    private static void processAnnotationsInClass(boolean key, Class<?> cls, QueryEntityTypeDescriptor type,
        @Nullable QueryEntityClassProperty parent) {
        if (U.isJdk(cls) || QueryUtils.isGeometryClass(cls)) {
            if (parent == null && !key && QueryUtils.isSqlType(cls)) { // We have to index primitive _val.
                String idxName = cls.getSimpleName() + "_" + QueryUtils.VAL_FIELD_NAME + "_idx";

                type.addIndex(idxName, QueryUtils.isGeometryClass(cls) ?
                    QueryIndexType.GEOSPATIAL : QueryIndexType.SORTED, QueryIndex.DFLT_INLINE_SIZE);

                type.addFieldToIndex(idxName, QueryUtils.VAL_FIELD_NAME, 0, false);
            }

            return;
        }

        if (parent != null && parent.knowsClass(cls))
            throw new CacheException("Recursive reference found in type: " + cls.getName());

        if (parent == null) { // Check class annotation at top level only.
            QueryTextField txtAnnCls = cls.getAnnotation(QueryTextField.class);

            if (txtAnnCls != null)
                type.valueTextIndex(true);

            QueryGroupIndex grpIdx = cls.getAnnotation(QueryGroupIndex.class);

            if (grpIdx != null)
                type.addIndex(grpIdx.name(), QueryIndexType.SORTED, grpIdx.inlineSize());

            QueryGroupIndex.List grpIdxList = cls.getAnnotation(QueryGroupIndex.List.class);

            if (grpIdxList != null && !F.isEmpty(grpIdxList.value())) {
                for (QueryGroupIndex idx : grpIdxList.value())
                    type.addIndex(idx.name(), QueryIndexType.SORTED, idx.inlineSize());
            }
        }

        for (Class<?> c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
            for (Field field : c.getDeclaredFields()) {
                QuerySqlField sqlAnn = field.getAnnotation(QuerySqlField.class);
                QueryTextField txtAnn = field.getAnnotation(QueryTextField.class);

                if (sqlAnn != null || txtAnn != null) {
                    QueryEntityClassProperty prop = new QueryEntityClassProperty(field);

                    prop.parent(parent);

                    // Add parent property before its possible nested properties so that
                    // resulting parent column comes before columns corresponding to those
                    // nested properties in the resulting table - that way nested
                    // properties override will happen properly (first parent, then children).
                    type.addProperty(prop, key, true);

                    processAnnotation(key, sqlAnn, txtAnn, cls, c, field.getType(), prop, type);
                }
            }
        }
    }

    /**
     * Processes annotation at field or method.
     *
     * @param key If given class relates to key.
     * @param sqlAnn SQL annotation, can be {@code null}.
     * @param txtAnn H2 text annotation, can be {@code null}.
     * @param cls Entity class.
     * @param curCls Current entity class.
     * @param fldCls Class of field or return type for method.
     * @param prop Current property.
     * @param desc Class description.
     */
    private static void processAnnotation(boolean key, QuerySqlField sqlAnn, QueryTextField txtAnn,
        Class<?> cls, Class<?> curCls, Class<?> fldCls, QueryEntityClassProperty prop, QueryEntityTypeDescriptor desc) {
        if (sqlAnn != null) {
            processAnnotationsInClass(key, fldCls, desc, prop);

            if (!sqlAnn.name().isEmpty())
                prop.alias(sqlAnn.name());

            if (sqlAnn.index()) {
                String idxName = curCls.getSimpleName() + "_" + prop.alias() + "_idx";

                if (cls != curCls)
                    idxName = cls.getSimpleName() + "_" + idxName;

                desc.addIndex(idxName, QueryUtils.isGeometryClass(prop.type()) ?
                    QueryIndexType.GEOSPATIAL : QueryIndexType.SORTED, sqlAnn.inlineSize());

                desc.addFieldToIndex(idxName, prop.fullName(), 0, sqlAnn.descending());
            }

            if (sqlAnn.notNull())
                desc.addNotNullField(prop.fullName());

            if (sqlAnn.precision() != -1)
                desc.addPrecision(prop.fullName(), sqlAnn.precision());

            if (sqlAnn.scale() != -1)
                desc.addScale(prop.fullName(), sqlAnn.scale());

            if ((!F.isEmpty(sqlAnn.groups()) || !F.isEmpty(sqlAnn.orderedGroups()))
                && sqlAnn.inlineSize() != QueryIndex.DFLT_INLINE_SIZE) {
                throw new CacheException("Inline size cannot be set on a field with group index [" +
                    "type=" + cls.getName() + ", property=" + prop.fullName() + ']');
            }

            if (!F.isEmpty(sqlAnn.groups())) {
                for (String group : sqlAnn.groups())
                    desc.addFieldToIndex(group, prop.fullName(), 0, false);
            }

            if (!F.isEmpty(sqlAnn.orderedGroups())) {
                for (QuerySqlField.Group idx : sqlAnn.orderedGroups())
                    desc.addFieldToIndex(idx.name(), prop.fullName(), idx.order(), idx.descending());
            }
        }

        if (txtAnn != null)
            desc.addFieldToTextIndex(prop.fullName());
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        QueryEntity entity = (QueryEntity)o;

        return F.eq(keyType, entity.keyType) &&
            F.eq(valType, entity.valType) &&
            F.eq(keyFieldName, entity.keyFieldName) &&
            F.eq(valueFieldName, entity.valueFieldName) &&
            F.eq(fields, entity.fields) &&
            F.eq(keyFields, entity.keyFields) &&
            F.eq(aliases, entity.aliases) &&
            F.eqNotOrdered(idxs, entity.idxs) &&
            F.eq(tableName, entity.tableName) &&
            F.eq(_notNullFields, entity._notNullFields) &&
            F.eq(defaultFieldValues, entity.defaultFieldValues) &&
            F.eq(fieldsPrecision, entity.fieldsPrecision) &&
            F.eq(fieldsScale, entity.fieldsScale);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(keyType, valType, keyFieldName, valueFieldName, fields, keyFields, aliases, idxs,
            tableName, _notNullFields, defaultFieldValues, fieldsPrecision, fieldsScale);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryEntity.class, this);
    }
}
