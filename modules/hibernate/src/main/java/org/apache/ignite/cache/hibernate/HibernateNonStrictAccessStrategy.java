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

package org.apache.ignite.cache.hibernate;

import java.util.Map;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.GridLeanSet;
import org.apache.ignite.internal.util.typedef.F;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.SoftLock;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link AccessType#NONSTRICT_READ_WRITE} cache access strategy.
 * <p>
 * Configuration of L2 cache and per-entity cache access strategy can be set in the
 * Hibernate configuration file:
 * <pre name="code" class="xml">
 * &lt;hibernate-configuration&gt;
 *     &lt;!-- Enable L2 cache. --&gt;
 *     &lt;property name="cache.use_second_level_cache"&gt;true&lt;/property&gt;
 *
 *     &lt;!-- Use Ignite as L2 cache provider. --&gt;
 *     &lt;property name="cache.region.factory_class"&gt;org.apache.ignite.cache.hibernate.HibernateRegionFactory&lt;/property&gt;
 *
 *     &lt;!-- Specify entity. --&gt;
 *     &lt;mapping class="com.example.Entity"/&gt;
 *
 *     &lt;!-- Enable L2 cache with nonstrict-read-write access strategy for entity. --&gt;
 *     &lt;class-cache class="com.example.Entity" usage="nonstrict-read-write"/&gt;
 * &lt;/hibernate-configuration&gt;
 * </pre>
 * Also cache access strategy can be set using annotations:
 * <pre name="code" class="java">
 * &#064;javax.persistence.Entity
 * &#064;javax.persistence.Cacheable
 * &#064;org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
 * public class Entity { ... }
 * </pre>
 */
public class HibernateNonStrictAccessStrategy extends HibernateAccessStrategyAdapter {
    /** */
    private final ThreadLocal<WriteContext> writeCtx;

    /**
     * @param ignite Grid.
     * @param cache Cache.
     * @param writeCtx Thread local instance used to track updates done during one Hibernate transaction.
     */
    protected HibernateNonStrictAccessStrategy(Ignite ignite, IgniteInternalCache<Object, Object> cache, ThreadLocal writeCtx) {
        super(ignite, cache);

        this.writeCtx = (ThreadLocal<WriteContext>)writeCtx;
    }

    /** {@inheritDoc} */
    @Nullable @Override protected SoftLock lock(Object key) throws CacheException {
        WriteContext ctx = writeCtx.get();

        if (ctx == null)
            writeCtx.set(ctx = new WriteContext());

        ctx.locked(key);

        return null;
    }

    /** {@inheritDoc} */
    @Override protected void unlock(Object key, SoftLock lock) throws CacheException {
        try {
            WriteContext ctx = writeCtx.get();

            if (ctx != null && ctx.unlocked(key)) {
                writeCtx.remove();

                ctx.updateCache(cache);
            }
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean update(Object key, Object val) throws CacheException {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean afterUpdate(Object key, Object val, SoftLock lock) throws CacheException {
        WriteContext ctx = writeCtx.get();

        if (ctx != null) {
            ctx.updated(key, val);

            unlock(key, lock);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean insert(Object key, Object val) throws CacheException {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean afterInsert(Object key, Object val) throws CacheException {
        try {
            cache.put(key, val);

            return true;
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void remove(Object key) throws CacheException {
        WriteContext ctx = writeCtx.get();

        if (ctx != null)
            ctx.removed(key);
    }

    /**
     * Information about updates done during single database transaction.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private static class WriteContext {
        /** */
        private Map<Object, Object> updates;

        /** */
        private Set<Object> rmvs;

        /** */
        private Set<Object> locked = new GridLeanSet<>();

        /**
         * Marks key as locked.
         *
         * @param key Key.
         */
        void locked(Object key) {
            locked.add(key);
        }

        /**
         * Marks key as unlocked.
         *
         * @param key Key.
         * @return {@code True} if last locked key was unlocked.
         */
        boolean unlocked(Object key) {
            locked.remove(key);

            return locked.isEmpty();
        }

        /**
         * Marks key as updated.
         *
         * @param key Key.
         * @param val Value.
         */
        void updated(Object key, Object val) {
            if (updates == null)
                updates = new GridLeanMap<>();

            updates.put(key, val);
        }

        /**
         * Marks key as removed.
         *
         * @param key Key.
         */
        void removed(Object key) {
            if (rmvs == null)
                rmvs = new GridLeanSet<>();

            rmvs.add(key);
        }

        /**
         * Updates cache.
         *
         * @param cache Cache.
         * @throws IgniteCheckedException If failed.
         */
        void updateCache(IgniteInternalCache<Object, Object> cache) throws IgniteCheckedException {
            if (!F.isEmpty(rmvs))
                cache.removeAll(rmvs);

            if (!F.isEmpty(updates))
                cache.putAll(updates);
        }
    }
}