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
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDefaultAffinityKeyMapper;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.processors.query.property.QueryBinaryProperty;
import org.apache.ignite.internal.processors.query.property.QueryClassProperty;
import org.apache.ignite.internal.processors.query.property.QueryFieldAccessor;
import org.apache.ignite.internal.processors.query.property.QueryMethodsAccessor;
import org.apache.ignite.internal.processors.query.property.QueryPropertyAccessor;
import org.apache.ignite.internal.processors.query.property.QueryReadOnlyMethodsAccessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_INDEXING_DISCOVERY_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.getInteger;

/**
 * Utility methods for queries.
 */
public class QueryUtils {

    /** Field name for key. */
    public static final String KEY_FIELD_NAME = "_KEY";

    /** Field name for value. */
    public static final String VAL_FIELD_NAME = "_VAL";

    /** Version field name. */
    public static final String VER_FIELD_NAME = "_VER";

    /** Discovery history size. */
    private static final int DISCO_HIST_SIZE = getInteger(IGNITE_INDEXING_DISCOVERY_HISTORY_SIZE, 1000);

    /** */
    private static final Class<?> GEOMETRY_CLASS = U.classForName("com.vividsolutions.jts.geom.Geometry", null);

    /** */
    private static final Set<Class<?>> SQL_TYPES = new HashSet<>(F.<Class<?>>asList(
        Integer.class,
        Boolean.class,
        Byte.class,
        Short.class,
        Long.class,
        BigDecimal.class,
        Double.class,
        Float.class,
        Time.class,
        Timestamp.class,
        java.util.Date.class,
        java.sql.Date.class,
        String.class,
        UUID.class,
        byte[].class
    ));

    /**
     * Get table name for entity.
     *
     * @param entity Entity.
     * @return Table name.
     */
    public static String tableName(QueryEntity entity) {
        String res = entity.getTableName();

        if (res == null) {
            String valTyp = entity.findValueType();

            if (valTyp == null)
                throw new IgniteException("Value type cannot be null or empty [queryEntity=" + entity + ']');

            res = typeName(entity.findValueType());
        }

        return res;
    }

    /**
     * Get index name.
     *
     * @param entity Query entity.
     * @param idx Index.
     * @return Index name.
     */
    public static String indexName(QueryEntity entity, QueryIndex idx) {
        return indexName(tableName(entity), idx);
    }

    /**
     * Get index name.
     *
     * @param tblName Table name.
     * @param idx Index.
     * @return Index name.
     */
    public static String indexName(String tblName, QueryIndex idx) {
        String res = idx.getName();

        if (res == null) {
            StringBuilder idxName = new StringBuilder(tblName + "_");

            for (Map.Entry<String, Boolean> field : idx.getFields().entrySet()) {
                idxName.append(field.getKey());

                idxName.append('_');
                idxName.append(field.getValue() ? "asc_" : "desc_");
            }

            for (int i = 0; i < idxName.length(); i++) {
                char ch = idxName.charAt(i);

                if (Character.isWhitespace(ch))
                    idxName.setCharAt(i, '_');
                else
                    idxName.setCharAt(i, Character.toLowerCase(ch));
            }

            idxName.append("idx");

            return idxName.toString();
        }

        return res;
    }

    /**
     * Normalize query entity. If "escape" flag is set, nothing changes. Otherwise we convert all object names to
     * upper case and replace inner class separator characters ('$' for Java and '.' for .NET) with underscore.
     *
     * @param entity Query entity.
     * @param escape Escape flag taken form configuration.
     * @return Normalized query entity.
     */
    public static QueryEntity normalizeQueryEntity(QueryEntity entity, boolean escape) {
        if (escape) {
            String tblName = tableName(entity);

            entity.setTableName(tblName);

            Map<String, String> aliases = new HashMap<>(entity.getAliases());

            for (String fieldName : entity.getFields().keySet()) {
                String fieldAlias = entity.getAliases().get(fieldName);

                if (fieldAlias == null) {
                    fieldAlias = aliasForFieldName(fieldName);

                    aliases.put(fieldName, fieldAlias);
                }
            }

            entity.setAliases(aliases);

            for (QueryIndex idx : entity.getIndexes())
                idx.setName(indexName(tblName, idx));

            validateQueryEntity(entity);

            return entity;
        }

        QueryEntity normalEntity = new QueryEntity();

        // Propagate plain properties.
        normalEntity.setKeyType(entity.getKeyType());
        normalEntity.setValueType(entity.getValueType());
        normalEntity.setFields(entity.getFields());
        normalEntity.setKeyFields(entity.getKeyFields());
        normalEntity.setKeyFieldName(entity.getKeyFieldName());
        normalEntity.setValueFieldName(entity.getValueFieldName());

        // Normalize table name.
        String normalTblName = entity.getTableName();

        if (normalTblName == null)
            // Replace special characters for auto-generated table name.
            normalTblName = normalizeObjectName(tableName(entity), true);
        else
            // No replaces for manually defined table.
            normalTblName = normalizeObjectName(normalTblName, false);

        normalEntity.setTableName(normalTblName);

        // Normalize field names through aliases.
        Map<String, String> normalAliases = new HashMap<>(normalEntity.getAliases());

        for (String fieldName : normalEntity.getFields().keySet()) {
            String fieldAlias = entity.getAliases().get(fieldName);

            if (fieldAlias == null)
                fieldAlias = aliasForFieldName(fieldName);

            assert fieldAlias != null;

            normalAliases.put(fieldName, normalizeObjectName(fieldAlias, false));
        }

        normalEntity.setAliases(normalAliases);

        // Normalize indexes.
        Collection<QueryIndex> normalIdxs = new LinkedList<>();

        for (QueryIndex idx : entity.getIndexes()) {
            QueryIndex normalIdx = new QueryIndex();

            normalIdx.setFields(idx.getFields());
            normalIdx.setIndexType(idx.getIndexType());
            normalIdx.setInlineSize(idx.getInlineSize());

            normalIdx.setName(normalizeObjectName(indexName(normalTblName, idx), false));

            normalIdxs.add(normalIdx);
        }

        normalEntity.setIndexes(normalIdxs);

        validateQueryEntity(normalEntity);

        return normalEntity;
    }

    /**
     * Stores rule for constructing schemaName according to cache configuration.
     *
     * @param cacheName Cache name.
     * @param schemaName Schema name.
     * @param escape Whether to perform escape.
     * @return Proper schema name according to ANSI-99 standard.
     */
    public static String normalizeSchemaName(String cacheName, @Nullable String schemaName, boolean escape) {
        String res = schemaName;

        if (res == null) {
            res = cacheName;

            // If schema name is not set explicitly, we will use escaped cache name. The reason is that cache name
            // could contain weird characters, such as underscores, dots or non-Latin stuff, which are invalid from
            // SQL synthax perspective. We do not want node to fail on startup due to this.
            escape = true;
        }

        if (!escape)
            res = normalizeObjectName(res, false);

        return res;
    }

    /**
     * Get alias for the field name (i.e. last part of the property).
     *
     * @param fieldName Field name.
     * @return Alias.
     */
    private static String aliasForFieldName(String fieldName) {
        int idx = fieldName.lastIndexOf('.');

        if (idx >= 0)
            fieldName = fieldName.substring(idx + 1);

        return fieldName;
    }

    /**
     * Normalize object name.
     *
     * @param str String.
     * @param replace Whether to perform replace.
     * @return Escaped string.
     */
    public static @Nullable String normalizeObjectName(@Nullable String str, boolean replace) {
        if (str == null)
            return null;

        if (replace)
            str = str.replace('.', '_').replace('$', '_');

        return str.toUpperCase();
    }

    /**
     * Create type candidate for query entity.
     *
     * @param cacheName Cache name.
     * @param cctx Cache context.
     * @param qryEntity Query entity.
     * @param mustDeserializeClss Classes which must be deserialized.
     * @param escape Escape flag.
     * @return Type candidate.
     * @throws IgniteCheckedException If failed.
     */
    public static QueryTypeCandidate typeForQueryEntity(String cacheName, GridCacheContext cctx, QueryEntity qryEntity,
        List<Class<?>> mustDeserializeClss, boolean escape) throws IgniteCheckedException {
        GridKernalContext ctx = cctx.kernalContext();
        CacheConfiguration<?,?> ccfg = cctx.config();

        boolean binaryEnabled = ctx.cacheObjects().isBinaryEnabled(ccfg);

        CacheObjectContext coCtx = binaryEnabled ? ctx.cacheObjects().contextForCache(ccfg) : null;

        QueryTypeDescriptorImpl desc = new QueryTypeDescriptorImpl(cacheName);

        desc.aliases(qryEntity.getAliases());

        // Key and value classes still can be available if they are primitive or JDK part.
        // We need that to set correct types for _key and _val columns.
        Class<?> keyCls = U.classForName(qryEntity.findKeyType(), null);
        Class<?> valCls = U.classForName(qryEntity.findValueType(), null);

        // If local node has the classes and they are externalizable, we must use reflection properties.
        boolean keyMustDeserialize = mustDeserializeBinary(ctx, keyCls);
        boolean valMustDeserialize = mustDeserializeBinary(ctx, valCls);

        boolean keyOrValMustDeserialize = keyMustDeserialize || valMustDeserialize;

        if (keyCls == null)
            keyCls = Object.class;

        String simpleValType = ((valCls == null) ? typeName(qryEntity.findValueType()) : typeName(valCls));

        desc.name(simpleValType);

        desc.tableName(qryEntity.getTableName());

        if (binaryEnabled && !keyOrValMustDeserialize) {
            // Safe to check null.
            if (SQL_TYPES.contains(valCls))
                desc.valueClass(valCls);
            else
                desc.valueClass(Object.class);

            if (SQL_TYPES.contains(keyCls))
                desc.keyClass(keyCls);
            else
                desc.keyClass(Object.class);
        }
        else {
            if (valCls == null)
                throw new IgniteCheckedException("Failed to find value class in the node classpath " +
                    "(use default marshaller to enable binary objects) : " + qryEntity.findValueType());

            desc.valueClass(valCls);
            desc.keyClass(keyCls);
        }

        desc.keyTypeName(qryEntity.findKeyType());
        desc.valueTypeName(qryEntity.findValueType());

        desc.keyFieldName(qryEntity.getKeyFieldName());
        desc.valueFieldName(qryEntity.getValueFieldName());

        if (binaryEnabled && keyOrValMustDeserialize) {
            if (keyMustDeserialize)
                mustDeserializeClss.add(keyCls);

            if (valMustDeserialize)
                mustDeserializeClss.add(valCls);
        }

        QueryTypeIdKey typeId;
        QueryTypeIdKey altTypeId = null;

        if (valCls == null || (binaryEnabled && !keyOrValMustDeserialize)) {
            processBinaryMeta(ctx, qryEntity, desc);

            typeId = new QueryTypeIdKey(cacheName, ctx.cacheObjects().typeId(qryEntity.findValueType()));

            if (valCls != null)
                altTypeId = new QueryTypeIdKey(cacheName, valCls);

            if (!cctx.customAffinityMapper() && qryEntity.findKeyType() != null) {
                // Need to setup affinity key for distributed joins.
                String affField = ctx.cacheObjects().affinityField(qryEntity.findKeyType());

                if (affField != null) {
                    if (!escape)
                        affField = normalizeObjectName(affField, false);

                    desc.affinityKey(affField);
                }
            }
        }
        else {
            processClassMeta(qryEntity, desc, coCtx);

            AffinityKeyMapper keyMapper = cctx.config().getAffinityMapper();

            if (keyMapper instanceof GridCacheDefaultAffinityKeyMapper) {
                String affField =
                    ((GridCacheDefaultAffinityKeyMapper)keyMapper).affinityKeyPropertyName(desc.keyClass());

                if (affField != null) {
                    if (!escape)
                        affField = normalizeObjectName(affField, false);

                    desc.affinityKey(affField);
                }
            }

            typeId = new QueryTypeIdKey(cacheName, valCls);
            altTypeId = new QueryTypeIdKey(cacheName, ctx.cacheObjects().typeId(qryEntity.findValueType()));
        }

        return new QueryTypeCandidate(typeId, altTypeId, desc);
    }

    /**
     * Processes declarative metadata for binary object.
     *
     * @param ctx Kernal context.
     * @param qryEntity Declared metadata.
     * @param d Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    public static void processBinaryMeta(GridKernalContext ctx, QueryEntity qryEntity, QueryTypeDescriptorImpl d)
        throws IgniteCheckedException {
        Set<String> keyFields = qryEntity.getKeyFields();

        // We have to distinguish between empty and null keyFields when the key is not of SQL type -
        // when a key is not of SQL type, absence of a field in nonnull keyFields tell us that this field
        // is a value field, and null keyFields tells us that current configuration
        // does not tell us anything about this field's ownership.
        boolean hasKeyFields = (keyFields != null);

        boolean isKeyClsSqlType = isSqlType(d.keyClass());

        if (hasKeyFields && !isKeyClsSqlType) {
            //ensure that 'keyFields' is case sensitive subset of 'fields'
            for (String keyField : keyFields) {
                if (!qryEntity.getFields().containsKey(keyField))
                    throw new IgniteCheckedException("QueryEntity 'keyFields' property must be a subset of keys " +
                        "from 'fields' property (case sensitive): " + keyField);
            }
        }

        for (Map.Entry<String, String> entry : qryEntity.getFields().entrySet()) {
            Boolean isKeyField;

            if (isKeyClsSqlType) // We don't care about keyFields in this case - it might be null, or empty, or anything
                isKeyField = false;
            else
                isKeyField = (hasKeyFields ? keyFields.contains(entry.getKey()) : null);

            QueryBinaryProperty prop = buildBinaryProperty(ctx, entry.getKey(),
                U.classForName(entry.getValue(), Object.class, true), d.aliases(), isKeyField);

            d.addProperty(prop, false);
        }

        processIndexes(qryEntity, d);
    }

    /**
     * Processes declarative metadata for binary object.
     *
     * @param qryEntity Declared metadata.
     * @param d Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    public static void processClassMeta(QueryEntity qryEntity, QueryTypeDescriptorImpl d, CacheObjectContext coCtx)
        throws IgniteCheckedException {
        for (Map.Entry<String, String> entry : qryEntity.getFields().entrySet()) {
            GridQueryProperty prop = buildProperty(
                d.keyClass(),
                d.valueClass(),
                d.keyFieldName(),
                d.valueFieldName(),
                entry.getKey(),
                U.classForName(entry.getValue(), Object.class),
                d.aliases(),
                coCtx);

            d.addProperty(prop, false);
        }

        processIndexes(qryEntity, d);
    }

    /**
     * Processes indexes based on query entity.
     *
     * @param qryEntity Query entity to process.
     * @param d Type descriptor to populate.
     * @throws IgniteCheckedException If failed to build index information.
     */
    private static void processIndexes(QueryEntity qryEntity, QueryTypeDescriptorImpl d) throws IgniteCheckedException {
        if (!F.isEmpty(qryEntity.getIndexes())) {
            for (QueryIndex idx : qryEntity.getIndexes())
                processIndex(idx, d);
        }
    }

    /**
     * Process dynamic index change.
     *
     * @param idx Index.
     * @param d Type descriptor to populate.
     * @throws IgniteCheckedException If failed to build index information.
     */
    public static void processDynamicIndexChange(String idxName, @Nullable QueryIndex idx, QueryTypeDescriptorImpl d)
        throws IgniteCheckedException {
        d.dropIndex(idxName);

        if (idx != null)
            processIndex(idx, d);
    }

    /**
     * Create index descriptor.
     *
     * @param typeDesc Type descriptor.
     * @param idx Index.
     * @return Index descriptor.
     * @throws IgniteCheckedException If failed.
     */
    public static QueryIndexDescriptorImpl createIndexDescriptor(QueryTypeDescriptorImpl typeDesc, QueryIndex idx)
        throws IgniteCheckedException {
        String idxName = indexName(typeDesc.tableName(), idx);
        QueryIndexType idxTyp = idx.getIndexType();

        assert idxTyp == QueryIndexType.SORTED || idxTyp == QueryIndexType.GEOSPATIAL;

        QueryIndexDescriptorImpl res = new QueryIndexDescriptorImpl(typeDesc, idxName, idxTyp, idx.getInlineSize());

        int i = 0;

        for (Map.Entry<String, Boolean> entry : idx.getFields().entrySet()) {
            String field = entry.getKey();
            boolean asc = entry.getValue();

            String alias = typeDesc.aliases().get(field);

            if (alias != null)
                field = alias;

            res.addField(field, i++, !asc);
        }

        return res;
    }

    /**
     * Process single index.
     *
     * @param idx Index.
     * @param d Type descriptor to populate.
     * @throws IgniteCheckedException If failed to build index information.
     */
    private static void processIndex(QueryIndex idx, QueryTypeDescriptorImpl d) throws IgniteCheckedException {
        QueryIndexType idxTyp = idx.getIndexType();

        if (idxTyp == QueryIndexType.SORTED || idxTyp == QueryIndexType.GEOSPATIAL) {
            QueryIndexDescriptorImpl idxDesc = createIndexDescriptor(d, idx);

            d.addIndex(idxDesc);
        }
        else if (idxTyp == QueryIndexType.FULLTEXT){
            for (String field : idx.getFields().keySet()) {
                String alias = d.aliases().get(field);

                if (alias != null)
                    field = alias;

                d.addFieldToTextIndex(field);
            }
        }
        else if (idxTyp != null)
            throw new IllegalArgumentException("Unsupported index type [idx=" + idx.getName() +
                ", typ=" + idxTyp + ']');
        else
            throw new IllegalArgumentException("Index type is not set: " + idx.getName());
    }

    /**
     * Builds binary object property.
     *
     * @param ctx Kernal context.
     * @param pathStr String representing path to the property. May contains dots '.' to identify
     *      nested fields.
     * @param resType Result type.
     * @param aliases Aliases.
     * @param isKeyField Key ownership flag, as defined in {@link QueryEntity#keyFields}: {@code true} if field belongs
     *      to key, {@code false} if it belongs to value, {@code null} if QueryEntity#keyFields is null.
     * @return Binary property.
     */
    public static QueryBinaryProperty buildBinaryProperty(GridKernalContext ctx, String pathStr, Class<?> resType,
                                     Map<String, String> aliases, @Nullable Boolean isKeyField) throws IgniteCheckedException {
        String[] path = pathStr.split("\\.");

        QueryBinaryProperty res = null;

        StringBuilder fullName = new StringBuilder();

        for (String prop : path) {
            if (fullName.length() != 0)
                fullName.append('.');

            fullName.append(prop);

            String alias = aliases.get(fullName.toString());

            // The key flag that we've found out is valid for the whole path.
            res = new QueryBinaryProperty(ctx, prop, res, resType, isKeyField, alias);
        }

        return res;
    }

    /**
     * @param keyCls Key class.
     * @param valCls Value class.
     * @param pathStr Path string.
     * @param resType Result type.
     * @param aliases Aliases.
     * @return Class property.
     * @throws IgniteCheckedException If failed.
     */
    public static QueryClassProperty buildClassProperty(Class<?> keyCls, Class<?> valCls, String pathStr,
        Class<?> resType, Map<String,String> aliases, CacheObjectContext coCtx) throws IgniteCheckedException {
        QueryClassProperty res = buildClassProperty(
            true,
            keyCls,
            pathStr,
            resType,
            aliases,
            coCtx);

        if (res == null) // We check key before value consistently with BinaryProperty.
            res = buildClassProperty(false, valCls, pathStr, resType, aliases, coCtx);

        if (res == null)
            throw new IgniteCheckedException(propertyInitializationExceptionMessage(keyCls, valCls, pathStr, resType));

        return res;
    }

    /**
     * @param keyCls Key class.
     * @param valCls Value class.
     * @param keyFieldName Key Field.
     * @param valueFieldName Value Field.
     * @param pathStr Path string.
     * @param resType Result type.
     * @param aliases Aliases.
     * @return Class property.
     * @throws IgniteCheckedException If failed.
     */
    public static GridQueryProperty buildProperty(Class<?> keyCls, Class<?> valCls, String keyFieldName, String valueFieldName, String pathStr,
                                                  Class<?> resType, Map<String,String> aliases, CacheObjectContext coCtx) throws IgniteCheckedException {
        if (pathStr.equals(keyFieldName))
            return new KeyOrValProperty(true, pathStr, keyCls);

        if (pathStr.equals(valueFieldName))
            return new KeyOrValProperty(false, pathStr, valCls);

        return buildClassProperty(keyCls,
                valCls,
                pathStr,
                resType,
                aliases,
                coCtx);
    }

    /**
     * Exception message to compare in tests.
     *
     * @param keyCls key class
     * @param valCls value class
     * @param pathStr property name
     * @param resType property type
     * @return Exception message.
     */
    public static String propertyInitializationExceptionMessage(Class<?> keyCls, Class<?> valCls, String pathStr,
        Class<?> resType) {
        return "Failed to initialize property '" + pathStr + "' of type '" +
            resType.getName() + "' for key class '" + keyCls + "' and value class '" + valCls + "'. " +
            "Make sure that one of these classes contains respective getter method or field.";
    }

    /**
     * @param key If this is a key property.
     * @param cls Source type class.
     * @param pathStr String representing path to the property. May contains dots '.' to identify nested fields.
     * @param resType Expected result type.
     * @param aliases Aliases.
     * @return Property instance corresponding to the given path.
     */
    @SuppressWarnings("ConstantConditions")
    public static QueryClassProperty buildClassProperty(boolean key, Class<?> cls, String pathStr, Class<?> resType,
        Map<String,String> aliases, CacheObjectContext coCtx) {
        String[] path = pathStr.split("\\.");

        QueryClassProperty res = null;

        StringBuilder fullName = new StringBuilder();

        for (String prop : path) {
            if (fullName.length() != 0)
                fullName.append('.');

            fullName.append(prop);

            String alias = aliases.get(fullName.toString());

            QueryPropertyAccessor accessor = findProperty(prop, cls);

            if (accessor == null)
                return null;

            QueryClassProperty tmp = new QueryClassProperty(accessor, key, alias, coCtx);

            tmp.parent(res);

            cls = tmp.type();

            res = tmp;
        }

        if (!U.box(resType).isAssignableFrom(U.box(res.type())))
            return null;

        return res;
    }

    /**
     * Find a member (either a getter method or a field) with given name of given class.
     * @param prop Property name.
     * @param cls Class to search for a member in.
     * @return Member for given name.
     */
    @Nullable private static QueryPropertyAccessor findProperty(String prop, Class<?> cls) {
        StringBuilder getBldr = new StringBuilder("get");
        getBldr.append(prop);
        getBldr.setCharAt(3, Character.toUpperCase(getBldr.charAt(3)));

        StringBuilder setBldr = new StringBuilder("set");
        setBldr.append(prop);
        setBldr.setCharAt(3, Character.toUpperCase(setBldr.charAt(3)));

        try {
            Method getter = cls.getMethod(getBldr.toString());

            Method setter;

            try {
                // Setter has to have the same name like 'setXxx' and single param of the same type
                // as the return type of the getter.
                setter = cls.getMethod(setBldr.toString(), getter.getReturnType());
            }
            catch (NoSuchMethodException ignore) {
                // Have getter, but no setter - return read-only accessor.
                return new QueryReadOnlyMethodsAccessor(getter, prop);
            }

            return new QueryMethodsAccessor(getter, setter, prop);
        }
        catch (NoSuchMethodException ignore) {
            // No-op.
        }

        getBldr = new StringBuilder("is");
        getBldr.append(prop);
        getBldr.setCharAt(2, Character.toUpperCase(getBldr.charAt(2)));

        // We do nothing about setBldr here as it corresponds to setProperty name which is what we need
        // for boolean property setter as well
        try {
            Method getter = cls.getMethod(getBldr.toString());

            Method setter;

            try {
                // Setter has to have the same name like 'setXxx' and single param of the same type
                // as the return type of the getter.
                setter = cls.getMethod(setBldr.toString(), getter.getReturnType());
            }
            catch (NoSuchMethodException ignore) {
                // Have getter, but no setter - return read-only accessor.
                return new QueryReadOnlyMethodsAccessor(getter, prop);
            }

            return new QueryMethodsAccessor(getter, setter, prop);
        }
        catch (NoSuchMethodException ignore) {
            // No-op.
        }

        Class cls0 = cls;

        while (cls0 != null)
            try {
                return new QueryFieldAccessor(cls0.getDeclaredField(prop));
            }
            catch (NoSuchFieldException ignored) {
                cls0 = cls0.getSuperclass();
            }

        try {
            Method getter = cls.getMethod(prop);

            Method setter;

            try {
                // Setter has to have the same name and single param of the same type
                // as the return type of the getter.
                setter = cls.getMethod(prop, getter.getReturnType());
            }
            catch (NoSuchMethodException ignore) {
                // Have getter, but no setter - return read-only accessor.
                return new QueryReadOnlyMethodsAccessor(getter, prop);
            }

            return new QueryMethodsAccessor(getter, setter, prop);
        }
        catch (NoSuchMethodException ignored) {
            // No-op.
        }

        // No luck.
        return null;
    }

    /**
     * Check whether type still must be deserialized when binary marshaller is set.
     *
     * @param ctx Kernal context.
     * @param cls Class.
     * @return {@code True} if will be deserialized.
     */
    private static boolean mustDeserializeBinary(GridKernalContext ctx, Class cls) {
        if (cls != null && cls != Object.class && ctx.config().getMarshaller() instanceof BinaryMarshaller) {
            CacheObjectBinaryProcessorImpl proc0 = (CacheObjectBinaryProcessorImpl)ctx.cacheObjects();

            return proc0.binaryContext().mustDeserialize(cls);
        }
        else
            return false;
    }

    /**
     * Checks if the given class can be mapped to a simple SQL type.
     *
     * @param cls Class.
     * @return {@code true} If can.
     */
    public static boolean isSqlType(Class<?> cls) {
        cls = U.box(cls);

        return SQL_TYPES.contains(cls) || QueryUtils.isGeometryClass(cls);
    }

    /**
     * Checks if the given class is GEOMETRY.
     *
     * @param cls Class.
     * @return {@code true} If this is geometry.
     */
    public static boolean isGeometryClass(Class<?> cls) {
        return GEOMETRY_CLASS != null && GEOMETRY_CLASS.isAssignableFrom(cls);
    }

    /**
     * Gets type name by class.
     *
     * @param clsName Class name.
     * @return Type name.
     */
    public static String typeName(String clsName) {
        int pkgEnd = clsName.lastIndexOf('.');

        if (pkgEnd >= 0 && pkgEnd < clsName.length() - 1)
            clsName = clsName.substring(pkgEnd + 1);

        if (clsName.endsWith("[]"))
            clsName = clsName.substring(0, clsName.length() - 2) + "_array";

        int parentEnd = clsName.lastIndexOf('$');

        if (parentEnd >= 0)
            clsName = clsName.substring(parentEnd + 1);

        parentEnd = clsName.lastIndexOf('+');   // .NET parent

        if (parentEnd >= 0)
            clsName = clsName.substring(parentEnd + 1);

        return clsName;
    }

    /**
     * Gets type name by class.
     *
     * @param cls Class.
     * @return Type name.
     */
    public static String typeName(Class<?> cls) {
        String typeName = cls.getSimpleName();

        // To protect from failure on anonymous classes.
        if (F.isEmpty(typeName)) {
            String pkg = cls.getPackage().getName();

            typeName = cls.getName().substring(pkg.length() + (pkg.isEmpty() ? 0 : 1));
        }

        if (cls.isArray()) {
            assert typeName.endsWith("[]");

            typeName = typeName.substring(0, typeName.length() - 2) + "_array";
        }

        return typeName;
    }

    /**
     * @param timeout Timeout.
     * @param timeUnit Time unit.
     * @return Converted time.
     */
    public static int validateTimeout(int timeout, TimeUnit timeUnit) {
        A.ensure(timeUnit != TimeUnit.MICROSECONDS && timeUnit != TimeUnit.NANOSECONDS,
            "timeUnit minimal resolution is millisecond.");

        A.ensure(timeout >= 0, "timeout value should be non-negative.");

        long tmp = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);

        return (int) tmp;
    }

    /**
     * @param ccfg Cache configuration.
     * @return {@code true} If query index must be enabled for this cache.
     */
    public static boolean isEnabled(CacheConfiguration<?,?> ccfg) {
        return !F.isEmpty(ccfg.getIndexedTypes()) ||
            !F.isEmpty(ccfg.getQueryEntities());
    }

    /**
     * Discovery history size.
     *
     * @return Discovery history size.
     */
    public static int discoveryHistorySize() {
        return DISCO_HIST_SIZE;
    }

    /**
     * Wrap schema exception if needed.
     *
     * @param e Original exception.
     * @return Schema exception.
     */
    @Nullable public static SchemaOperationException wrapIfNeeded(@Nullable Exception e) {
        if (e == null)
            return null;

        if (e instanceof SchemaOperationException)
            return (SchemaOperationException)e;

        return new SchemaOperationException("Unexpected exception.", e);
    }

    /**
     * Validate query entity.
     *
     * @param entity Entity.
     */
    private static void validateQueryEntity(QueryEntity entity) {
        if (F.isEmpty(entity.findValueType()))
            throw new IgniteException("Value type cannot be null or empty [queryEntity=" + entity + ']');

        String keyFieldName = entity.getKeyFieldName();

        if (keyFieldName != null && !entity.getFields().containsKey(keyFieldName)) {
            throw new IgniteException("Key field is not in the field list [queryEntity=" + entity +
                ", keyFieldName=" + keyFieldName + "]");
        }

        String valFieldName = entity.getValueFieldName();

        if (valFieldName != null && !entity.getFields().containsKey(valFieldName)) {
            throw new IgniteException("Value field is not in the field list [queryEntity=" + entity +
                ", valFieldName=" + valFieldName + "]");
        }

        Collection<QueryIndex> idxs = entity.getIndexes();

        if (!F.isEmpty(idxs)) {
            Set<String> idxNames = new HashSet<>();

            for (QueryIndex idx : idxs) {
                String idxName = idx.getName();

                if (idxName == null)
                    idxName = indexName(entity, idx);

                assert !F.isEmpty(idxName);

                if (!idxNames.add(idxName))
                    throw new IgniteException("Duplicate index name [queryEntity=" + entity +
                        ", queryIdx=" + idx + ']');

                if (idx.getIndexType() == null)
                    throw new IgniteException("Index type is not set [queryEntity=" + entity +
                        ", queryIdx=" + idx + ']');
            }
        }
    }

    /**
     * Private constructor.
     */
    private QueryUtils() {
        // No-op.
    }

    /** Property used for keyFieldName or valueFieldName */
    public static class KeyOrValProperty implements GridQueryProperty {
        /** */
        boolean isKey;

        /** */
        String name;

        /** */
        Class<?> cls;

        /** */
        public KeyOrValProperty(boolean key, String name, Class<?> cls) {
            this.isKey = key;
            this.name = name;
            this.cls = cls;
        }

        /** {@inheritDoc} */
        @Override public Object value(Object key, Object val) throws IgniteCheckedException {
            return isKey ? key : val;
        }

        /** {@inheritDoc} */
        @Override public void setValue(Object key, Object val, Object propVal) throws IgniteCheckedException {
            //No-op
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public Class<?> type() {
            return cls;
        }

        /** {@inheritDoc} */
        @Override public boolean key() {
            return isKey;
        }

        /** {@inheritDoc} */
        @Override public GridQueryProperty parent() {
            return null;
        }
    }
}
