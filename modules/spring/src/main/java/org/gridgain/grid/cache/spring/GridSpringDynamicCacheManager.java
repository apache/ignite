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

package org.gridgain.grid.cache.spring;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.springframework.cache.*;
import org.springframework.cache.annotation.*;

import java.io.*;
import java.util.*;

/**
 * Extension of {@link GridSpringCacheManager} that adds an option to
 * emulate dynamic cache creation for you Spring-based applications.
 * <p>
 * All the data will be actually cached in one GridGain cache. It's
 * name should be provided to this cache manager via
 * {@link #setDataCacheName(String)} configuration property.
 * <p>
 * Under the hood, this cache manager will create a cache projection
 * for each cache name provided in {@link Cacheable}, {@link CachePut},
 * etc. annotations. Note that you're still able to use caches configured in
 * GridGain configuration. Cache projection will be created only
 * cache with provided name doesn't exist.
 * <h1 class="header">Configuration</h1>
 * {@link GridSpringDynamicCacheManager} inherits all configuration
 * properties from {@link GridSpringCacheManager} (see it's JavaDoc
 * for more information on how to enable GridGain-based caching in
 * a Spring application).
 * <p>
 * Additionally you will need to set a GridGain cache name where the data for
 * all dynamically created caches will be stored. By default its name
 * is {@code null}, which refers to default cache. Here is the example
 * of how to configure a named cache:
 * <pre name="code" class="xml">
 * &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *        xmlns:cache="http://www.springframework.org/schema/cache"
 *        xsi:schemaLocation="
 *         http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 *         http://www.springframework.org/schema/cache http://www.springframework.org/schema/cache/spring-cache.xsd"&gt;
 *     &lt;-- Provide configuration file path --&gt;
 *     &lt;bean id="cacheManager" class="org.gridgain.grid.cache.spring.GridSpringCacheManager"&gt;
 *         &lt;property name="dataCacheName" value="myDataCache"/&gt;
 *     &lt;/bean>
 *
 *     ...
 * &lt;/beans&gt;
 * </pre>
 *
 * @see GridSpringCacheManager
 */
public class GridSpringDynamicCacheManager extends GridSpringCacheManager {
    /** Data cache name. */
    private String dataCacheName;

    /** Meta cache. */
    private GridCacheProjectionEx<MetaKey, Cache> metaCache;

    /** Data cache. */
    private GridCache<DataKey, Object> dataCache;

    /**
     * Sets data cache name.
     *
     * @return Data cache name.
     */
    public String getDataCacheName() {
        return dataCacheName;
    }

    /**
     * Gets data cache name.
     *
     * @param dataCacheName Data cache name.
     */
    public void setDataCacheName(String dataCacheName) {
        this.dataCacheName = dataCacheName;
    }

    /** {@inheritDoc} */
    @Override public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();

        metaCache = ((GridEx)grid).utilityCache(MetaKey.class, Cache.class);
        dataCache = grid.cache(dataCacheName);
    }

    /** {@inheritDoc} */
    @Override public Cache getCache(final String name) {
        Cache cache = super.getCache(name);

        if (cache != null)
            return cache;

        try {
            MetaKey key = new MetaKey(name);

            cache = metaCache.get(key);

            if (cache == null) {
                cache = new GridSpringCache(name, grid, dataCache.projection(new ProjectionFilter(name)),
                    new IgniteClosure<Object, Object>() {
                        @Override public Object apply(Object o) {
                            return new DataKey(name, o);
                        }
                    });

                Cache old = metaCache.putIfAbsent(key, cache);

                if (old != null)
                    cache = old;
            }

            return cache;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<String> getCacheNames() {
        Collection<String> names = F.view(super.getCacheNames(), new IgnitePredicate<String>() {
            @Override public boolean apply(String name) {
                return !F.eq(name, dataCacheName);
            }
        });

        return F.concat(false, names, F.transform(metaCache.entrySetx(),
            new IgniteClosure<Map.Entry<MetaKey, Cache>, String>() {
                @Override public String apply(Map.Entry<MetaKey, Cache> e) {
                    return e.getKey().name;
                }
            }));
    }

    /**
     * Meta key.
     */
    private static class MetaKey extends GridCacheUtilityKey<MetaKey> implements Externalizable {
        /** Cache name. */
        private String name;

        /**
         * For {@link Externalizable}.
         */
        public MetaKey() {
            // No-op.
        }

        /**
         * @param name Cache name.
         */
        private MetaKey(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override protected boolean equalsx(MetaKey key) {
            return name != null ? name.equals(key.name) : key.name == null;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return name.hashCode();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, name);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            name = U.readString(in);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MetaKey.class, this);
        }
    }

    /**
     * Data key.
     */
    private static class DataKey implements Externalizable {
        /** Cache name. */
        private String name;

        /** Key. */
        @GridToStringInclude
        private Object key;

        /**
         * @param name Cache name.
         * @param key Key.
         */
        private DataKey(String name, Object key) {
            this.name = name;
            this.key = key;
        }

        /**
         * For {@link Externalizable}.
         */
        public DataKey() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            DataKey key0 = (DataKey)o;

            return name != null ? name.equals(key0.name) : key0.name == null &&
                key != null ? key.equals(key0.key) : key0.key == null;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = name != null ? name.hashCode() : 0;

            res = 31 * res + (key != null ? key.hashCode() : 0);

            return res;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, name);
            out.writeObject(key);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            name = U.readString(in);
            key = in.readObject();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DataKey.class, this);
        }
    }

    /**
     * Projection filter.
     */
    private static class ProjectionFilter implements IgniteBiPredicate<DataKey, Object>, Externalizable {
        /** Cache name. */
        private String name;

        /**
         * For {@link Externalizable}.
         */
        public ProjectionFilter() {
            // No-op.
        }

        /**
         * @param name Cache name.
         */
        private ProjectionFilter(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(DataKey key, Object val) {
            return name != null ? name.equals(key.name) : key.name == null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, name);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            name = U.readString(in);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ProjectionFilter.class, this);
        }
    }
}
