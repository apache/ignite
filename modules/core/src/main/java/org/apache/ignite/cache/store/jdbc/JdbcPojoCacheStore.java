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

import org.apache.ignite.*;
import org.apache.ignite.cache.store.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.lang.reflect.*;
import java.sql.*;
import java.util.*;

/**
 * Base class for {@link CacheStore} that implementation backed by JDBC and POJO via reflection.
 *
 * This implementation stores objects in underlying database using java beans mapping description via reflection.
 */
public class JdbcPojoCacheStore extends JdbcCacheStore<Object, Object> {
    /**
     * POJO methods cache.
     */
    protected static class PojoMethodsCache {
        /** POJO class. */
        protected final Class<?> cls;

        /** Constructor for POJO object. */
        private final Constructor ctor;

        /** Cached setters for POJO object. */
        private final Map<String, Method> getters;

        /** Cached getters for POJO object. */
        private final Map<String, Method> setters;

        /**
         * POJO methods cache.
         *
         * @param clsName Class name.
         * @param fields Fields.
         */
        public PojoMethodsCache(String clsName,
            Collection<GridCacheQueryTypeDescriptor> fields) throws IgniteCheckedException {

            try {
                cls = Class.forName(clsName);

                ctor = cls.getDeclaredConstructor();

                if (!ctor.isAccessible())
                    ctor.setAccessible(true);
            }
            catch (ClassNotFoundException e) {
                throw new IgniteCheckedException("Failed to find class: " + clsName, e);
            }
            catch (NoSuchMethodException e) {
                throw new IgniteCheckedException("Failed to find empty constructor for class: " + clsName, e);
            }

            setters = U.newHashMap(fields.size());

            getters = U.newHashMap(fields.size());

            for (GridCacheQueryTypeDescriptor field : fields) {
                String prop = capitalFirst(field.getJavaName());

                try {
                    getters.put(field.getJavaName(), cls.getMethod("get" + prop));
                }
                catch (NoSuchMethodException ignored) {
                    try {
                        getters.put(field.getJavaName(), cls.getMethod("is" + prop));
                    }
                    catch (NoSuchMethodException e) {
                        throw new IgniteCheckedException("Failed to find getter for property " + field.getJavaName() +
                            " of class: " + cls.getName(), e);
                    }
                }

                try {
                    setters.put(field.getJavaName(), cls.getMethod("set" + prop, field.getJavaType()));
                }
                catch (NoSuchMethodException e) {
                    throw new IgniteCheckedException("Failed to find setter for property " + field.getJavaName() +
                        " of class: " + clsName, e);
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

        /**
         * Construct new instance of pojo object.
         *
         * @return pojo object.
         * @throws IgniteCheckedException If construct new instance failed.
         */
        protected Object newInstance() throws IgniteCheckedException {
            try {
                return ctor.newInstance();
            }
            catch (Exception e) {
                throw new IgniteCheckedException("Failed to create new instance for class: " + cls, e);
            }
        }
    }

    /** Methods cache. */
    protected Map<String, PojoMethodsCache> mtdsCache;

    /** {@inheritDoc} */
    @Override protected void buildTypeCache() throws IgniteCheckedException {
        entryQtyCache = U.newHashMap(typeMetadata.size());

        mtdsCache = U.newHashMap(typeMetadata.size() * 2);

        for (GridCacheQueryTypeMetadata type : typeMetadata) {
            PojoMethodsCache keyCache = new PojoMethodsCache(type.getKeyType(), type.getKeyDescriptors());

            mtdsCache.put(type.getKeyType(), keyCache);

            entryQtyCache.put(keyCache.cls, new QueryCache(dialect, type));

            mtdsCache.put(type.getType(), new PojoMethodsCache(type.getType(), type.getValueDescriptors()));
        }
    }

    /** {@inheritDoc} */
    @Override protected <R> R buildObject(String typeName, Collection<GridCacheQueryTypeDescriptor> fields,
        ResultSet rs) throws IgniteCheckedException {
        PojoMethodsCache t = mtdsCache.get(typeName);

        Object obj = t.newInstance();

        try {
            for (GridCacheQueryTypeDescriptor field : fields)
                t.setters.get(field.getJavaName()).invoke(obj, rs.getObject(field.getDbName()));

            return (R)obj;
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to read object of class: " + typeName, e);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Object extractField(String typeName, String fieldName, Object obj)
        throws IgniteCheckedException {
        try {
            PojoMethodsCache mc = mtdsCache.get(typeName);

            return mc.getters.get(fieldName).invoke(obj);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to read object of class: " + typeName, e);
        }
    }

    /** {@inheritDoc} */
    @Override protected Object typeKey(Object key) {
        return key.getClass();
    }
}
