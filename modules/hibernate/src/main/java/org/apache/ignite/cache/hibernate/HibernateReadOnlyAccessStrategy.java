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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.SoftLock;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link AccessType#READ_ONLY} cache access strategy.
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
 *     &lt;!-- Enable L2 cache with read-only access strategy for entity. --&gt;
 *     &lt;class-cache class="com.example.Entity" usage="read-only"/&gt;
 * &lt;/hibernate-configuration&gt;
 * </pre>
 * Also cache access strategy can be set using annotations:
 * <pre name="code" class="java">
 * &#064;javax.persistence.Entity
 * &#064;javax.persistence.Cacheable
 * &#064;org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.READ_ONLY)
 * public class Entity { ... }
 * </pre>

 *
 */
public class HibernateReadOnlyAccessStrategy extends HibernateAccessStrategyAdapter {
    /**
     * @param ignite Grid.
     * @param cache Cache.
     */
    public HibernateReadOnlyAccessStrategy(Ignite ignite, IgniteInternalCache<Object, Object> cache) {
        super(ignite, cache);
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
    @Nullable @Override protected SoftLock lock(Object key) throws CacheException {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void unlock(Object key, SoftLock lock) throws CacheException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void remove(Object key) throws CacheException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected boolean update(Object key, Object val) throws CacheException {
        throw new UnsupportedOperationException("Updates are not supported for read-only access strategy.");
    }

    /** {@inheritDoc} */
    @Override protected boolean afterUpdate(Object key, Object val, SoftLock lock) throws CacheException {
        throw new UnsupportedOperationException("Updates are not supported for read-only access strategy.");
    }
}