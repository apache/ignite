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

package org.apache.ignite.cache.store.jdbc;

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.integration.*;
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;

/**
 * Implementation of {@link CacheStore} backed by JDBC and POJO via reflection.
 *
 * This implementation stores objects in underlying database using java beans mapping description via reflection.
 * <p>
 * Use {@link CacheJdbcPojoStoreFactory} factory to pass {@link CacheJdbcPojoStore} to {@link CacheConfiguration}.
 */
public class CacheJdbcPojoStore<K, V> extends CacheAbstractJdbcStore<K, V> {
    /**
     * POJO methods cache.
     */
    protected static class PojoMethodsCache {
        /** POJO class. */
        protected final Class<?> cls;

        /** Constructor for POJO object. */
        private Constructor ctor;

        /** {@code true} if object is a simple type. */
        private final boolean simple;

        /** Cached setters for POJO object. */
        private Map<String, Method> getters;

        /** Cached getters for POJO object. */
        private Map<String, Method> setters;

        /**
         * POJO methods cache.
         *
         * @param clsName Class name.
         * @param fields Fields.
         *
         * @throws CacheException If failed to construct type cache.
         */
        public PojoMethodsCache(String clsName, Collection<CacheTypeFieldMetadata> fields) throws CacheException {
            try {
                cls = Class.forName(clsName);

                if (simple = simpleType(cls))
                    return;

                ctor = cls.getDeclaredConstructor();

                if (!ctor.isAccessible())
                    ctor.setAccessible(true);
            }
            catch (ClassNotFoundException e) {
                throw new CacheException("Failed to find class: " + clsName, e);
            }
            catch (NoSuchMethodException e) {
                throw new CacheException("Failed to find default constructor for class: " + clsName, e);
            }

            setters = U.newHashMap(fields.size());

            getters = U.newHashMap(fields.size());

            for (CacheTypeFieldMetadata field : fields) {
                String prop = capitalFirst(field.getJavaName());

                try {
                    getters.put(field.getJavaName(), cls.getMethod("get" + prop));
                }
                catch (NoSuchMethodException ignored) {
                    try {
                        getters.put(field.getJavaName(), cls.getMethod("is" + prop));
                    }
                    catch (NoSuchMethodException e) {
                        throw new CacheException("Failed to find getter in POJO class [clsName=" + clsName +
                            ", prop=" + field.getJavaName() + "]", e);
                    }
                }

                try {
                    setters.put(field.getJavaName(), cls.getMethod("set" + prop, field.getJavaType()));
                }
                catch (NoSuchMethodException e) {
                    throw new CacheException("Failed to find setter in POJO class [clsName=" + clsName +
                        ", prop=" + field.getJavaName() + "]", e);
                }
            }
        }

        /**
         * Capitalizes the first character of the given string.
         *
         * @param str String.
         * @return String with capitalized first character.
         */
        @Nullable private String capitalFirst(@Nullable String str) {
            return str == null ? null :
                str.isEmpty() ? "" : Character.toUpperCase(str.charAt(0)) + str.substring(1);
        }
    }

    /** Methods cache. */
    protected volatile Map<String, Map<String, PojoMethodsCache>> mtdsCache = Collections.emptyMap();

    /** {@inheritDoc} */
    @Override protected void prepareBuilders(@Nullable String cacheName, Collection<CacheTypeMetadata> types)
        throws CacheException {
        Map<String, PojoMethodsCache> typeMethods = U.newHashMap(types.size() * 2);

        for (CacheTypeMetadata type : types) {
            String keyType = type.getKeyType();
            typeMethods.put(keyType, new PojoMethodsCache(keyType, type.getKeyFields()));

            String valType = type.getValueType();
            typeMethods.put(valType, new PojoMethodsCache(valType, type.getValueFields()));
        }

        Map<String, Map<String, PojoMethodsCache>> newMtdsCache = new HashMap<>(mtdsCache);

        newMtdsCache.put(cacheName, typeMethods);

        mtdsCache = newMtdsCache;
    }

    /** {@inheritDoc} */
    @Override protected <R> R buildObject(String cacheName, String typeName, Collection<CacheTypeFieldMetadata> fields,
        Map<String, Integer> loadColIdxs, ResultSet rs) throws CacheLoaderException {
        PojoMethodsCache mc = mtdsCache.get(cacheName).get(typeName);

        if (mc == null)
            throw new CacheLoaderException("Failed to find cache type metadata for type: " + typeName);

        try {
            if (mc.simple) {
                CacheTypeFieldMetadata field = F.first(fields);

                return (R)getColumnValue(rs, loadColIdxs.get(field.getDatabaseName()), mc.cls);
            }

            Object obj = mc.ctor.newInstance();

            for (CacheTypeFieldMetadata field : fields) {
                String fldJavaName = field.getJavaName();

                Method setter = mc.setters.get(fldJavaName);

                if (setter == null)
                    throw new IllegalStateException("Failed to find setter in POJO class [clsName=" + typeName +
                        ", prop=" + fldJavaName + "]");

                String fldDbName = field.getDatabaseName();

                Integer colIdx = loadColIdxs.get(fldDbName);

                try {
                    setter.invoke(obj, getColumnValue(rs, colIdx, field.getJavaType()));
                }
                catch (Exception e) {
                    throw new IllegalStateException("Failed to set property in POJO class [clsName=" + typeName +
                        ", prop=" + fldJavaName + ", col=" + colIdx + ", dbName=" + fldDbName + "]", e);
                }
            }

            return (R)obj;
        }
        catch (SQLException e) {
            throw new CacheLoaderException("Failed to read object of class: " + typeName, e);
        }
        catch (Exception e) {
            throw new CacheLoaderException("Failed to construct instance of class: " + typeName, e);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Object extractParameter(String cacheName, String typeName, String fieldName,
        Object obj)
        throws CacheException {
        try {
            PojoMethodsCache mc = mtdsCache.get(cacheName).get(typeName);

            if (mc == null)
                throw new CacheException("Failed to find cache type metadata for type: " + typeName);

            if (mc.simple)
                return obj;

            Method getter = mc.getters.get(fieldName);

            if (getter == null)
                throw new CacheLoaderException("Failed to find getter in POJO class [clsName=" + typeName +
                    ", prop=" + fieldName + "]");

            return getter.invoke(obj);
        }
        catch (Exception e) {
            throw new CacheException("Failed to read object of class: " + typeName, e);
        }
    }

    /** {@inheritDoc} */
    @Override protected Object keyTypeId(Object key) throws CacheException {
        return key.getClass();
    }

    /** {@inheritDoc} */
    @Override protected Object keyTypeId(String type) throws CacheException {
        try {
            return Class.forName(type);
        }
        catch (ClassNotFoundException e) {
            throw new CacheException("Failed to find class: " + type, e);
        }
    }
}
