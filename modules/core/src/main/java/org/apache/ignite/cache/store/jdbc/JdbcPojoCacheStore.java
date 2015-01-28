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

import org.apache.ignite.cache.query.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.integration.*;
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
        public PojoMethodsCache(String clsName, Collection<CacheQueryTypeDescriptor> fields) throws CacheException {

            try {
                cls = Class.forName(clsName);

                ctor = cls.getDeclaredConstructor();

                if (!ctor.isAccessible())
                    ctor.setAccessible(true);
            }
            catch (ClassNotFoundException e) {
                throw new CacheException("Failed to find class: " + clsName, e);
            }
            catch (NoSuchMethodException e) {
                throw new CacheException("Failed to find empty constructor for class: " + clsName, e);
            }

            setters = U.newHashMap(fields.size());

            getters = U.newHashMap(fields.size());

            for (CacheQueryTypeDescriptor field : fields) {
                String prop = capitalFirst(field.getJavaName());

                try {
                    getters.put(field.getJavaName(), cls.getMethod("get" + prop));
                }
                catch (NoSuchMethodException ignored) {
                    try {
                        getters.put(field.getJavaName(), cls.getMethod("is" + prop));
                    }
                    catch (NoSuchMethodException e) {
                        throw new CacheException("Failed to find getter for property " + field.getJavaName() +
                            " of class: " + cls.getName(), e);
                    }
                }

                try {
                    setters.put(field.getJavaName(), cls.getMethod("set" + prop, field.getJavaType()));
                }
                catch (NoSuchMethodException e) {
                    throw new CacheException("Failed to find setter for property " + field.getJavaName() +
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
         * @throws CacheLoaderException If construct new instance failed.
         */
        protected Object newInstance() throws CacheLoaderException {
            try {
                return ctor.newInstance();
            }
            catch (Exception e) {
                throw new CacheLoaderException("Failed to create new instance for class: " + cls, e);
            }
        }
    }

    /** Methods cache. */
    protected Map<String, PojoMethodsCache> mtdsCache;

    /** {@inheritDoc} */
    @Override protected void buildTypeCache() throws CacheException {
        entryMappings = U.newHashMap(typeMetadata.size());

        mtdsCache = U.newHashMap(typeMetadata.size() * 2);

        for (CacheQueryTypeMetadata type : typeMetadata) {
            PojoMethodsCache keyCache = new PojoMethodsCache(type.getKeyType(), type.getKeyDescriptors());

            mtdsCache.put(type.getKeyType(), keyCache);

            entryMappings.put(new IgniteBiTuple<String, Object>(null, keyId(type.getKeyType())),
                new EntryMapping(dialect, type));

            mtdsCache.put(type.getType(), new PojoMethodsCache(type.getType(), type.getValueDescriptors()));
        }

        entryMappings = Collections.unmodifiableMap(entryMappings);

        mtdsCache = Collections.unmodifiableMap(mtdsCache);
    }

    /** {@inheritDoc} */
    @Override protected <R> R buildObject(String typeName, Collection<CacheQueryTypeDescriptor> fields,
        ResultSet rs) throws CacheLoaderException {
        PojoMethodsCache t = mtdsCache.get(typeName);

        Object obj = t.newInstance();

        try {
            for (CacheQueryTypeDescriptor field : fields)
                t.setters.get(field.getJavaName()).invoke(obj, rs.getObject(field.getDbName()));

            return (R)obj;
        }
        catch (Exception e) {
            throw new CacheLoaderException("Failed to read object of class: " + typeName, e);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Object extractField(String typeName, String fieldName, Object obj)
        throws CacheException {
        try {
            PojoMethodsCache mc = mtdsCache.get(typeName);

            return mc.getters.get(fieldName).invoke(obj);
        }
        catch (Exception e) {
            throw new CacheException("Failed to read object of class: " + typeName, e);
        }
    }

    /** {@inheritDoc} */
    @Override protected Object keyId(Object key) throws CacheException {
        return key.getClass();
    }

    /** {@inheritDoc} */
    @Override protected Object keyId(String type) throws CacheException {
        try {
            return Class.forName(type);
        }
        catch (ClassNotFoundException e) {
            throw new CacheException("Failed to find class: " + type, e);
        }
    }
}
