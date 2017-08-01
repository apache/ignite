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
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import javax.cache.CacheException;
import org.apache.ignite.cache.query.annotations.QueryGroupIndex;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QueryTextField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
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
        this(convert(processKeyAndValueClasses(keyCls,valCls)));
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
    public Collection<QueryIndex> getIndexes() {
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
     * @param tableName table name
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Utility method for building query entities programmatically.
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
     * @param keyCls Key class.
     * @param valCls Value class.
     * @return Type descriptor.
     */
    public static TypeDescriptor processKeyAndValueClasses(
        Class<?> keyCls,
        Class<?> valCls
    ) {
        TypeDescriptor d = new TypeDescriptor();

        d.keyClass(keyCls);
        d.valueClass(valCls);

        processAnnotationsInClass(true, d.keyCls, d, null);
        processAnnotationsInClass(false, d.valCls, d, null);

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
    private static void processAnnotationsInClass(boolean key, Class<?> cls, TypeDescriptor type,
        @Nullable ClassProperty parent) {
        if (U.isJdk(cls) || QueryUtils.isGeometryClass(cls)) {
            if (parent == null && !key && QueryUtils.isSqlType(cls)) { // We have to index primitive _val.
                String idxName = cls.getSimpleName() + "_" + QueryUtils.VAL_FIELD_NAME + "_idx";

                type.addIndex(idxName, QueryUtils.isGeometryClass(cls) ?
                    QueryIndexType.GEOSPATIAL : QueryIndexType.SORTED);

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
                type.addIndex(grpIdx.name(), QueryIndexType.SORTED);

            QueryGroupIndex.List grpIdxList = cls.getAnnotation(QueryGroupIndex.List.class);

            if (grpIdxList != null && !F.isEmpty(grpIdxList.value())) {
                for (QueryGroupIndex idx : grpIdxList.value())
                    type.addIndex(idx.name(), QueryIndexType.SORTED);
            }
        }

        for (Class<?> c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
            for (Field field : c.getDeclaredFields()) {
                QuerySqlField sqlAnn = field.getAnnotation(QuerySqlField.class);
                QueryTextField txtAnn = field.getAnnotation(QueryTextField.class);

                if (sqlAnn != null || txtAnn != null) {
                    ClassProperty prop = new ClassProperty(field);

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
        Class<?> cls, Class<?> curCls, Class<?> fldCls, ClassProperty prop, TypeDescriptor desc) {
        if (sqlAnn != null) {
            processAnnotationsInClass(key, fldCls, desc, prop);

            if (!sqlAnn.name().isEmpty())
                prop.alias(sqlAnn.name());

            if (sqlAnn.index()) {
                String idxName = curCls.getSimpleName() + "_" + prop.alias() + "_idx";

                if (cls != curCls)
                    idxName = cls.getSimpleName() + "_" + idxName;

                desc.addIndex(idxName, QueryUtils.isGeometryClass(prop.type()) ?
                    QueryIndexType.GEOSPATIAL : QueryIndexType.SORTED);

                desc.addFieldToIndex(idxName, prop.fullName(), 0, sqlAnn.descending());
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

    /**
     * @param desc Type descriptor.
     * @return Type metadata.
     */
    public static QueryEntity convert(TypeDescriptor desc) {
        QueryEntity entity = new QueryEntity();

        // Key and val types.
        entity.setKeyType(desc.keyClass().getName());
        entity.setValueType(desc.valueClass().getName());

        for (ClassProperty prop : desc.props.values())
            entity.addQueryField(prop.fullName(), U.box(prop.type()).getName(), prop.alias());

        entity.setKeyFields(desc.keyProps);

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

        return entity;
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
            F.eq(tableName, entity.tableName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(keyType, valType, keyFieldName, valueFieldName, fields, keyFields, aliases, idxs, tableName);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryEntity.class, this);
    }

    /**
     * Descriptor of type.
     */
    private static class TypeDescriptor {
        /** Value field names and types with preserved order. */
        @GridToStringInclude
        private final Map<String, Class<?>> fields = new LinkedHashMap<>();

        /** */
        @GridToStringExclude
        private final Map<String, ClassProperty> props = new LinkedHashMap<>();

        /** */
        @GridToStringInclude
        private final Set<String> keyProps = new HashSet<>();

        /** */
        @GridToStringInclude
        private final Map<String, IndexDescriptor> indexes = new HashMap<>();

        /** */
        private IndexDescriptor fullTextIdx;

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
        public IndexDescriptor addIndex(String idxName, QueryIndexType type, int inlineSize) {
            IndexDescriptor idx = new IndexDescriptor(type, inlineSize);

            if (indexes.put(idxName, idx) != null)
                throw new CacheException("Index with name '" + idxName + "' already exists.");

            return idx;
        }

        /**
         * Adds index.
         *
         * @param idxName Index name.
         * @param type Index type.
         * @return Index descriptor.
         */
        public IndexDescriptor addIndex(String idxName, QueryIndexType type) {
            return addIndex(idxName, type, -1);
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
            IndexDescriptor desc = indexes.get(idxName);

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
                fullTextIdx = new IndexDescriptor(QueryIndexType.FULLTEXT);

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
        void valueClass(Class<?> valCls) {
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
        void keyClass(Class<?> keyCls) {
            this.keyCls = keyCls;
        }

        /**
         * Adds property to the type descriptor.
         *
         * @param prop Property.
         * @param key Property ownership flag (key or not).
         * @param failOnDuplicate Fail on duplicate flag.
         */
        void addProperty(ClassProperty prop, boolean key, boolean failOnDuplicate) {
            String name = prop.fullName();

            if (props.put(name, prop) != null && failOnDuplicate)
                throw new CacheException("Property with name '" + name + "' already exists.");

            fields.put(name, prop.type());

            if (key)
                keyProps.add(name);
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
            return S.toString(TypeDescriptor.class, this);
        }
    }

    /**
     * Index descriptor.
     */
    private static class IndexDescriptor implements GridQueryIndexDescriptor {
        /** Fields sorted by order number. */
        private final Collection<T2<String, Integer>> fields = new TreeSet<>(
            new Comparator<T2<String, Integer>>() {
                @Override public int compare(T2<String, Integer> o1, T2<String, Integer> o2) {
                    if (o1.get2().equals(o2.get2())) // Order is equal, compare field names to avoid replace in Set.
                        return o1.get1().compareTo(o2.get1());

                    return o1.get2() < o2.get2() ? -1 : 1;
                }
            });

        /** Fields which should be indexed in descending order. */
        private Collection<String> descendings;

        /** */
        private final QueryIndexType type;

        /** */
        private final int inlineSize;

        /**
         * @param type Type.
         * @param inlineSize Inline size.
         */
        private IndexDescriptor(QueryIndexType type, int inlineSize) {
            assert type != null;

            this.type = type;
            this.inlineSize = inlineSize;
        }

        /**
         * @param type Type.
         */
        private IndexDescriptor(QueryIndexType type) {
            this(type, -1);
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<String> fields() {
            Collection<String> res = new ArrayList<>(fields.size());

            for (T2<String, Integer> t : fields)
                res.add(t.get1());

            return res;
        }

        /** {@inheritDoc} */
        @Override public boolean descending(String field) {
            return descendings != null && descendings.contains(field);
        }

        /**
         * Adds field to this index.
         *
         * @param field Field name.
         * @param orderNum Field order number in this index.
         * @param descending Sort order.
         */
        public void addField(String field, int orderNum, boolean descending) {
            fields.add(new T2<>(field, orderNum));

            if (descending) {
                if (descendings == null)
                    descendings = new HashSet<>();

                descendings.add(field);
            }
        }

        /** {@inheritDoc} */
        @Override public QueryIndexType type() {
            return type;
        }

        /** {@inheritDoc} */
        @Override public int inlineSize() {
            return inlineSize;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IndexDescriptor.class, this);
        }
    }

    /**
     * Description of type property.
     */
    private static class ClassProperty {
        /** */
        private final Member member;

        /** */
        private ClassProperty parent;

        /** */
        private String name;

        /** */
        private String alias;

        /**
         * Constructor.
         *
         * @param member Element.
         */
        ClassProperty(Member member) {
            this.member = member;

            name = member.getName();

            if (member instanceof Method) {
                if (member.getName().startsWith("get") && member.getName().length() > 3)
                    name = member.getName().substring(3);

                if (member.getName().startsWith("is") && member.getName().length() > 2)
                    name = member.getName().substring(2);
            }

            ((AccessibleObject)member).setAccessible(true);
        }

        /**
         * @param alias Alias.
         */
        public void alias(String alias) {
            this.alias = alias;
        }

        /**
         * @return Alias.
         */
        String alias() {
            return F.isEmpty(alias) ? name : alias;
        }

        /**
         * @return Type.
         */
        public Class<?> type() {
            return member instanceof Field ? ((Field)member).getType() : ((Method)member).getReturnType();
        }

        /**
         * @param parent Parent property if this is embeddable element.
         */
        public void parent(ClassProperty parent) {
            this.parent = parent;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ClassProperty.class, this);
        }

        /**
         * @param cls Class.
         * @return {@code true} If this property or some parent relates to member of the given class.
         */
        public boolean knowsClass(Class<?> cls) {
            return member.getDeclaringClass() == cls || (parent != null && parent.knowsClass(cls));
        }

        /**
         * @return Full name with all parents in dot notation.
         */
        public String fullName() {
            assert name != null;

            if (parent == null)
                return name;

            return parent.fullName() + '.' + name;
        }
    }
}
