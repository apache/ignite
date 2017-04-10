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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Utility methods for queries.
 */
public class QueryUtils {
    /** */
    public static final String _VAL = "_val";

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
     * Create type candidate for query entity.
     *
     * @param space Space.
     * @param cctx Cache context.
     * @param qryEntity Query entity.
     * @param mustDeserializeClss Classes which must be deserialized.
     * @return Type candidate.
     * @throws IgniteCheckedException If failed.
     */
    public static QueryTypeCandidate typeForQueryEntity(String space, GridCacheContext cctx, QueryEntity qryEntity,
        List<Class<?>> mustDeserializeClss) throws IgniteCheckedException {
        if (F.isEmpty(qryEntity.getValueType()))
            throw new IgniteCheckedException("Value type is not set: " + qryEntity);

        GridKernalContext ctx = cctx.kernalContext();
        CacheConfiguration<?,?> ccfg = cctx.config();

        boolean binaryEnabled = ctx.cacheObjects().isBinaryEnabled(ccfg);

        CacheObjectContext coCtx = binaryEnabled ? ctx.cacheObjects().contextForCache(ccfg) : null;

        QueryTypeDescriptorImpl desc = new QueryTypeDescriptorImpl();

        // Key and value classes still can be available if they are primitive or JDK part.
        // We need that to set correct types for _key and _val columns.
        Class<?> keyCls = U.classForName(qryEntity.getKeyType(), null);
        Class<?> valCls = U.classForName(qryEntity.getValueType(), null);

        // If local node has the classes and they are externalizable, we must use reflection properties.
        boolean keyMustDeserialize = mustDeserializeBinary(ctx, keyCls);
        boolean valMustDeserialize = mustDeserializeBinary(ctx, valCls);

        boolean keyOrValMustDeserialize = keyMustDeserialize || valMustDeserialize;

        if (keyCls == null)
            keyCls = Object.class;

        String simpleValType = ((valCls == null) ? typeName(qryEntity.getValueType()) : typeName(valCls));

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
                    "(use default marshaller to enable binary objects) : " + qryEntity.getValueType());

            desc.valueClass(valCls);
            desc.keyClass(keyCls);
        }

        desc.keyTypeName(qryEntity.getKeyType());
        desc.valueTypeName(qryEntity.getValueType());

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

            typeId = new QueryTypeIdKey(space, ctx.cacheObjects().typeId(qryEntity.getValueType()));

            if (valCls != null)
                altTypeId = new QueryTypeIdKey(space, valCls);

            if (!cctx.customAffinityMapper() && qryEntity.getKeyType() != null) {
                // Need to setup affinity key for distributed joins.
                String affField = ctx.cacheObjects().affinityField(qryEntity.getKeyType());

                if (affField != null)
                    desc.affinityKey(affField);
            }
        }
        else {
            processClassMeta(qryEntity, desc, coCtx);

            AffinityKeyMapper keyMapper = cctx.config().getAffinityMapper();

            if (keyMapper instanceof GridCacheDefaultAffinityKeyMapper) {
                String affField =
                    ((GridCacheDefaultAffinityKeyMapper)keyMapper).affinityKeyPropertyName(desc.keyClass());

                if (affField != null)
                    desc.affinityKey(affField);
            }

            typeId = new QueryTypeIdKey(space, valCls);
            altTypeId = new QueryTypeIdKey(space, ctx.cacheObjects().typeId(qryEntity.getValueType()));
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
        Map<String,String> aliases = qryEntity.getAliases();

        if (aliases == null)
            aliases = Collections.emptyMap();

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
                U.classForName(entry.getValue(), Object.class, true), aliases, isKeyField);

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
        Map<String,String> aliases = qryEntity.getAliases();

        if (aliases == null)
            aliases = Collections.emptyMap();

        for (Map.Entry<String, String> entry : qryEntity.getFields().entrySet()) {
            QueryClassProperty prop = buildClassProperty(
                d.keyClass(),
                d.valueClass(),
                entry.getKey(),
                U.classForName(entry.getValue(), Object.class),
                aliases,
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
            Map<String, String> aliases = qryEntity.getAliases();

            if (aliases == null)
                aliases = Collections.emptyMap();

            for (QueryIndex idx : qryEntity.getIndexes()) {
                String idxName = idx.getName();

                if (idxName == null)
                    idxName = QueryEntity.defaultIndexName(idx);

                QueryIndexType idxTyp = idx.getIndexType();

                if (idxTyp == QueryIndexType.SORTED || idxTyp == QueryIndexType.GEOSPATIAL) {
                    d.addIndex(idxName, idxTyp);

                    int i = 0;

                    for (Map.Entry<String, Boolean> entry : idx.getFields().entrySet()) {
                        String field = entry.getKey();
                        boolean asc = entry.getValue();

                        String alias = aliases.get(field);

                        if (alias != null)
                            field = alias;

                        d.addFieldToIndex(idxName, field, i++, !asc);
                    }
                }
                else if (idxTyp == QueryIndexType.FULLTEXT){
                    for (String field : idx.getFields().keySet()) {
                        String alias = aliases.get(field);

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
        }
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
     * Private constructor.
     */
    private QueryUtils() {
        // No-op.
    }
}
