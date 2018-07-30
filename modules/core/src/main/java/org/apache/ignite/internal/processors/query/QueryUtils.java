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

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.CacheDefaultBinaryAffinityKeyMapper;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDefaultAffinityKeyMapper;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.property.QueryBinaryProperty;
import org.apache.ignite.internal.processors.query.property.QueryClassProperty;
import org.apache.ignite.internal.processors.query.property.QueryFieldAccessor;
import org.apache.ignite.internal.processors.query.property.QueryMethodsAccessor;
import org.apache.ignite.internal.processors.query.property.QueryPropertyAccessor;
import org.apache.ignite.internal.processors.query.property.QueryReadOnlyMethodsAccessor;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.util.Jsr310Java8DateTimeApiUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_INDEXING_DISCOVERY_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.getInteger;

/**
 * Utility methods for queries.
 */
public class QueryUtils {
    /** Default schema. */
    public static final String DFLT_SCHEMA = "PUBLIC";

    /** Schema for system view. */
    public static final String SCHEMA_SYS = "IGNITE";

    /** Field name for key. */
    public static final String KEY_FIELD_NAME = "_KEY";

    /** Field name for value. */
    public static final String VAL_FIELD_NAME = "_VAL";

    /** Version field name. */
    public static final String VER_FIELD_NAME = "_VER";

    /** Well-known template name for PARTITIONED cache. */
    public static final String TEMPLATE_PARTITIONED = "PARTITIONED";

    /** Well-known template name for REPLICATED cache. */
    public static final String TEMPLATE_REPLICATED = "REPLICATED";

    /** Discovery history size. */
    private static final int DISCO_HIST_SIZE = getInteger(IGNITE_INDEXING_DISCOVERY_HISTORY_SIZE, 1000);

    /** */
    private static final Class<?> GEOMETRY_CLASS = U.classForName("com.vividsolutions.jts.geom.Geometry", null);

    /** */
    private static final Set<Class<?>> SQL_TYPES = createSqlTypes();

    /**
     * Creates SQL types set.
     *
     * @return SQL types set.
     */
    @NotNull private static Set<Class<?>> createSqlTypes() {
        Set<Class<?>> sqlClasses = new HashSet<>(Arrays.<Class<?>>asList(
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
            Date.class,
            java.sql.Date.class,
            String.class,
            UUID.class,
            byte[].class
        ));

        sqlClasses.addAll(Jsr310Java8DateTimeApiUtils.jsr310ApiClasses());

        return sqlClasses;
    }

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
     * Normalize cache query entities.
     *
     * @param entities Query entities.
     * @param cfg Cache config.
     * @return Normalized query entities.
     */
    public static Collection<QueryEntity> normalizeQueryEntities(Collection<QueryEntity> entities,
        CacheConfiguration<?, ?> cfg) {
        Collection<QueryEntity> normalEntities = new ArrayList<>(entities.size());

        for (QueryEntity entity : entities) {
            if (!F.isEmpty(entity.getNotNullFields()))
                checkNotNullAllowed(cfg);

            normalEntities.add(normalizeQueryEntity(entity, cfg.isSqlEscapeAll()));
        }

        return normalEntities;
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

        QueryEntity normalEntity = entity instanceof QueryEntityEx ? new QueryEntityEx() : new QueryEntity();

        // Propagate plain properties.
        normalEntity.setKeyType(entity.getKeyType());
        normalEntity.setValueType(entity.getValueType());
        normalEntity.setFields(entity.getFields());
        normalEntity.setKeyFields(entity.getKeyFields());
        normalEntity.setKeyFieldName(entity.getKeyFieldName());
        normalEntity.setValueFieldName(entity.getValueFieldName());
        normalEntity.setNotNullFields(entity.getNotNullFields());
        normalEntity.setDefaultFieldValues(entity.getDefaultFieldValues());
        normalEntity.setDecimalInfo(entity.getDecimalInfo());

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
     * @return Proper schema name according to ANSI-99 standard.
     */
    public static String normalizeSchemaName(String cacheName, @Nullable String schemaName) {
        boolean escape = false;

        String res = schemaName;

        if (res == null) {
            res = cacheName;

            // If schema name is not set explicitly, we will use escaped cache name. The reason is that cache name
            // could contain weird characters, such as underscores, dots or non-Latin stuff, which are invalid from
            // SQL synthax perspective. We do not want node to fail on startup due to this.
            escape = true;
        }
        else {
            if (res.startsWith("\"") && res.endsWith("\"")) {
                res = res.substring(1, res.length() - 1);

                escape = true;
            }
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
     * @param replace Whether to perform replace of special characters.
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
     * @param schemaName Schema name.
     * @param cctx Cache context.
     * @param qryEntity Query entity.
     * @param mustDeserializeClss Classes which must be deserialized.
     * @param escape Escape flag.
     * @return Type candidate.
     * @throws IgniteCheckedException If failed.
     */
    public static QueryTypeCandidate typeForQueryEntity(String cacheName, String schemaName, GridCacheContext cctx,
        QueryEntity qryEntity, List<Class<?>> mustDeserializeClss, boolean escape) throws IgniteCheckedException {
        GridKernalContext ctx = cctx.kernalContext();
        CacheConfiguration<?,?> ccfg = cctx.config();

        boolean binaryEnabled = ctx.cacheObjects().isBinaryEnabled(ccfg);

        CacheObjectContext coCtx = binaryEnabled ? ctx.cacheObjects().contextForCache(ccfg) : null;

        QueryTypeDescriptorImpl desc = new QueryTypeDescriptorImpl(cacheName);

        desc.schemaName(schemaName);

        desc.aliases(qryEntity.getAliases());

        // Key and value classes still can be available if they are primitive or JDK part.
        // We need that to set correct types for _key and _val columns.
        // We better box these types - otherwise, if user provides, say, raw 'byte' for
        // key or value (which they could), we'll deem key or value as Object which clearly is not right.
        Class<?> keyCls = U.box(U.classForName(qryEntity.findKeyType(), null, true));
        Class<?> valCls = U.box(U.classForName(qryEntity.findValueType(), null, true));

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

        int valTypeId = ctx.cacheObjects().typeId(qryEntity.findValueType());

        if (valCls == null || (binaryEnabled && !keyOrValMustDeserialize)) {
            processBinaryMeta(ctx, qryEntity, desc);

            typeId = new QueryTypeIdKey(cacheName, valTypeId);

            if (valCls != null)
                altTypeId = new QueryTypeIdKey(cacheName, valCls);

            String affField = null;

            // Need to setup affinity key for distributed joins.
            String keyType = qryEntity.getKeyType();

            if (!cctx.customAffinityMapper() && keyType != null) {
                if (coCtx != null) {
                    CacheDefaultBinaryAffinityKeyMapper mapper =
                        (CacheDefaultBinaryAffinityKeyMapper)coCtx.defaultAffMapper();

                    BinaryField field = mapper.affinityKeyField(keyType);

                    if (field != null)
                        affField = field.name();
                }
            }

            if (affField != null) {
                if (!escape)
                    affField = normalizeObjectName(affField, false);

                desc.affinityKey(affField);
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
            altTypeId = new QueryTypeIdKey(cacheName, valTypeId);
        }

        desc.typeId(valTypeId);

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
        Set<String> notNulls = qryEntity.getNotNullFields();
        Map<String, Object> dlftVals = qryEntity.getDefaultFieldValues();
        Map<String, IgniteBiTuple<Integer, Integer>> decimalInfo  = qryEntity.getDecimalInfo();

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

            boolean notNull = notNulls != null && notNulls.contains(entry.getKey());

            Object dfltVal = dlftVals != null ? dlftVals.get(entry.getKey()) : null;

            IgniteBiTuple<Integer, Integer> precisionAndScale =
                decimalInfo != null ? decimalInfo.get(entry.getKey()) : null;

            QueryBinaryProperty prop = buildBinaryProperty(ctx, entry.getKey(),
                U.classForName(entry.getValue(), Object.class, true),
                d.aliases(), isKeyField, notNull, dfltVal,
                precisionAndScale != null ? precisionAndScale.get1() : -1,
                precisionAndScale != null ? precisionAndScale.get2() : -1);

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
        Set<String> notNulls = qryEntity.getNotNullFields();

        for (Map.Entry<String, String> entry : qryEntity.getFields().entrySet()) {
            GridQueryProperty prop = buildProperty(
                d.keyClass(),
                d.valueClass(),
                d.keyFieldName(),
                d.valueFieldName(),
                entry.getKey(),
                U.classForName(entry.getValue(), Object.class),
                d.aliases(),
                notNulls != null && notNulls.contains(entry.getKey()),
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
     * @param notNull {@code true} if {@code null} value is not allowed.
     * @param dlftVal Default value.
     * @param precision Precision.
     * @param scale Scale.
     * @return Binary property.
     * @throws IgniteCheckedException On error.
     */
    public static QueryBinaryProperty buildBinaryProperty(GridKernalContext ctx, String pathStr, Class<?> resType,
        Map<String, String> aliases, @Nullable Boolean isKeyField, boolean notNull, Object dlftVal,
        int precision, int scale) throws IgniteCheckedException {
        String[] path = pathStr.split("\\.");

        QueryBinaryProperty res = null;

        StringBuilder fullName = new StringBuilder();

        for (String prop : path) {
            if (fullName.length() != 0)
                fullName.append('.');

            fullName.append(prop);

            String alias = aliases.get(fullName.toString());

            // The key flag that we've found out is valid for the whole path.
            res = new QueryBinaryProperty(ctx, prop, res, resType, isKeyField, alias, notNull, dlftVal,
                precision, scale);
        }

        return res;
    }

    /**
     * @param keyCls Key class.
     * @param valCls Value class.
     * @param pathStr Path string.
     * @param resType Result type.
     * @param aliases Aliases.
     * @param notNull {@code true} if {@code null} value is not allowed.
     * @param coCtx Cache object context.
     * @return Class property.
     * @throws IgniteCheckedException If failed.
     */
    public static QueryClassProperty buildClassProperty(Class<?> keyCls, Class<?> valCls, String pathStr,
        Class<?> resType, Map<String,String> aliases, boolean notNull, CacheObjectContext coCtx)
        throws IgniteCheckedException {
        QueryClassProperty res = buildClassProperty(
            true,
            keyCls,
            pathStr,
            resType,
            aliases,
            notNull,
            coCtx);

        if (res == null) // We check key before value consistently with BinaryProperty.
            res = buildClassProperty(false, valCls, pathStr, resType, aliases, notNull, coCtx);

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
     * @param notNull {@code true} if {@code null} value is not allowed.
     * @param coCtx Cache object context.
     * @return Class property.
     * @throws IgniteCheckedException If failed.
     */
    public static GridQueryProperty buildProperty(Class<?> keyCls, Class<?> valCls, String keyFieldName,
        String valueFieldName, String pathStr, Class<?> resType, Map<String,String> aliases, boolean notNull,
        CacheObjectContext coCtx) throws IgniteCheckedException {
        if (pathStr.equals(keyFieldName))
            return new KeyOrValProperty(true, pathStr, keyCls);

        if (pathStr.equals(valueFieldName))
            return new KeyOrValProperty(false, pathStr, valCls);

        return buildClassProperty(keyCls,
                valCls,
                pathStr,
                resType,
                aliases,
                notNull,
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
     * @param notNull {@code true} if {@code null} value is not allowed.
     * @param coCtx Cache object context.
     * @return Property instance corresponding to the given path.
     */
    @SuppressWarnings("ConstantConditions")
    public static QueryClassProperty buildClassProperty(boolean key, Class<?> cls, String pathStr, Class<?> resType,
        Map<String,String> aliases, boolean notNull, CacheObjectContext coCtx) {
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

            QueryClassProperty tmp = new QueryClassProperty(accessor, key, alias, notNull, coCtx);

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
     * Check given {@link CacheConfiguration} for conflicts in table and index names from any query entities
     *     found in collection of {@link DynamicCacheDescriptor}s and belonging to the same schema.
     *
     * @param ccfg New cache configuration.
     * @param descs Cache descriptors.
     * @return Exception message describing found conflict or {@code null} if none found.
     */
    public static SchemaOperationException checkQueryEntityConflicts(CacheConfiguration<?, ?> ccfg,
        Collection<DynamicCacheDescriptor> descs) {
        String schema = QueryUtils.normalizeSchemaName(ccfg.getName(), ccfg.getSqlSchema());

        Set<String> idxNames = new HashSet<>();

        Set<String> tblNames = new HashSet<>();

        for (DynamicCacheDescriptor desc : descs) {
            if (F.eq(ccfg.getName(), desc.cacheName()))
                continue;

            String descSchema = QueryUtils.normalizeSchemaName(desc.cacheName(),
                desc.cacheConfiguration().getSqlSchema());

            if (!F.eq(schema, descSchema))
                continue;

            for (QueryEntity e : desc.schema().entities()) {
                tblNames.add(e.getTableName());

                for (QueryIndex idx : e.getIndexes())
                    idxNames.add(idx.getName());
            }
        }

        for (QueryEntity e : ccfg.getQueryEntities()) {
            if (!tblNames.add(e.getTableName()))
                return new SchemaOperationException(SchemaOperationException.CODE_TABLE_EXISTS, e.getTableName());

            for (QueryIndex idx : e.getIndexes())
                if (!idxNames.add(idx.getName()))
                    return new SchemaOperationException(SchemaOperationException.CODE_INDEX_EXISTS, idx.getName());
        }

        return null;
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
     * Construct cache name for table.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @return Cache name.
     */
    public static String createTableCacheName(String schemaName, String tblName) {
        return "SQL_" + schemaName + "_" + tblName;
    }

    /**
     * Construct value type name for table.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @return Value type name.
     */
    public static String createTableValueTypeName(String schemaName, String tblName) {
        return createTableCacheName(schemaName, tblName) + "_" + UUID.randomUUID().toString().replace("-", "_");
    }

    /**
     * Construct key type name for table.
     *
     * @param valTypeName Value type name.
     * @return Key type name.
     */
    public static String createTableKeyTypeName(String valTypeName) {
        return valTypeName + "_KEY";
    }

    /**
     * Copy query entity.
     *
     * @param entity Query entity.
     * @return Copied entity.
     */
    public static QueryEntity copy(QueryEntity entity) {
        QueryEntity res;

        if (entity instanceof QueryEntityEx)
            res = new QueryEntityEx(entity);
        else
            res = new QueryEntity(entity);

        return res;
    }

    /**
     * Performs checks to forbid cache configurations that are not compatible with NOT NULL query fields.
     * See {@link QueryEntity#setNotNullFields(Set)}.
     *
     * @param cfg Cache configuration.
     */
    public static void checkNotNullAllowed(CacheConfiguration cfg) {
        if (cfg.isReadThrough())
            throw new IgniteSQLException("NOT NULL constraint is not supported when CacheConfiguration.readThrough " +
                "is enabled.", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        if (cfg.getInterceptor() != null)
            throw new IgniteSQLException("NOT NULL constraint is not supported when CacheConfiguration.interceptor " +
                "is set.", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /**
     * Checks if given column can be removed from table using its {@link QueryEntity}.
     *
     * @param entity Query entity.
     * @param fieldName Name of the field of the key or value object.
     * @param colName Name of the column.
     * @return {@code null} if it's OK to remove the column and exception otherwise.
     */
    public static SchemaOperationException validateDropColumn(QueryEntity entity, String fieldName, String colName) {
        if (F.eq(fieldName, entity.getKeyFieldName()) || KEY_FIELD_NAME.equalsIgnoreCase(fieldName))
            return new SchemaOperationException("Cannot drop column \"" + colName +
                "\" because it represents an entire cache key");

        if (F.eq(fieldName, entity.getValueFieldName()) || VAL_FIELD_NAME.equalsIgnoreCase(fieldName))
            return new SchemaOperationException("Cannot drop column \"" + colName +
                "\" because it represents an entire cache value");

        Set<String> keyFields = entity.getKeyFields();

        if (keyFields != null && keyFields.contains(fieldName))
            return new SchemaOperationException("Cannot drop column \"" + colName +
                "\" because it is a part of a cache key");

        Collection<QueryIndex> indexes = entity.getIndexes();

        if (indexes != null) {
            for (QueryIndex idxDesc : indexes) {
                if (idxDesc.getFields().containsKey(fieldName))
                    return new SchemaOperationException("Cannot drop column \"" + colName +
                        "\" because an index exists (\"" + idxDesc.getName() + "\") that uses the column.");
            }
        }

        return null;
    }

    /**
     * Checks if given column can be removed from the table using its {@link GridQueryTypeDescriptor}.
     *
     * @param type Type descriptor.
     * @param colName Name of the column.
     * @return {@code null} if it's OK to remove the column and exception otherwise.
     */
    public static SchemaOperationException validateDropColumn(GridQueryTypeDescriptor type, String colName) {
        if (F.eq(colName, type.keyFieldName()) || KEY_FIELD_NAME.equalsIgnoreCase(colName))
            return new SchemaOperationException("Cannot drop column \"" + colName +
                "\" because it represents an entire cache key");

        if (F.eq(colName, type.valueFieldName()) || VAL_FIELD_NAME.equalsIgnoreCase(colName))
            return new SchemaOperationException("Cannot drop column \"" + colName +
                "\" because it represents an entire cache value");

        GridQueryProperty prop = type.property(colName);

        if (prop != null && prop.key())
            return new SchemaOperationException("Cannot drop column \"" + colName +
                "\" because it is a part of a cache key");

        Collection<GridQueryIndexDescriptor> indexes = type.indexes().values();

        for (GridQueryIndexDescriptor idxDesc : indexes) {
            if (idxDesc.fields().contains(colName))
                return new SchemaOperationException("Cannot drop column \"" + colName +
                    "\" because an index exists (\"" + idxDesc.name() + "\") that uses the column.");
        }

        return null;
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

        /** {@inheritDoc} */
        @Override public boolean notNull() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public Object defaultValue() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public int precision() {
            return -1;
        }

        /** {@inheritDoc} */
        @Override public int scale() {
            return -1;
        }
    }
}
