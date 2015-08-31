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
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.EntityRegion;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.EntityRegionAccessStrategy;
import org.hibernate.cache.spi.access.SoftLock;

/**
 * Implementation of {@link EntityRegion}. This region is used to store entity data.
 * <p>
 * L2 cache for entity can be enabled in the Hibernate configuration file:
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
 * Also cache for entity can be enabled using annotations:
 * <pre name="code" class="java">
 * &#064;javax.persistence.Entity
 * &#064;javax.persistence.Cacheable
 * &#064;org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
 * public class Entity { ... }
 * </pre>
 */
public class HibernateEntityRegion extends HibernateTransactionalDataRegion implements EntityRegion {
    /**
     * @param factory Region factory.
     * @param name Region name.
     * @param ignite Grid.
     * @param cache Region cache,
     * @param dataDesc Region data description.
     */
    public HibernateEntityRegion(HibernateRegionFactory factory, String name, Ignite ignite,
        IgniteInternalCache<Object, Object> cache, CacheDataDescription dataDesc) {
        super(factory, name, ignite, cache, dataDesc);
    }

    /** {@inheritDoc} */
    @Override public EntityRegionAccessStrategy buildAccessStrategy(AccessType accessType) throws CacheException {
        return new AccessStrategy(createAccessStrategy(accessType));
    }

    /**
     * Entity region access strategy.
     */
    private class AccessStrategy extends HibernateAbstractRegionAccessStrategy
        implements EntityRegionAccessStrategy {
        /**
         * @param stgy Access strategy implementation.
         */
        private AccessStrategy(HibernateAccessStrategyAdapter stgy) {
            super(stgy);
        }

        /** {@inheritDoc} */
        @Override public EntityRegion getRegion() {
            return HibernateEntityRegion.this;
        }

        /** {@inheritDoc} */
        @Override public boolean insert(Object key, Object val, Object ver) throws CacheException {
            return stgy.insert(key, val);
        }

        /** {@inheritDoc} */
        @Override public boolean afterInsert(Object key, Object val, Object ver) throws CacheException {
            return stgy.afterInsert(key, val);
        }

        /** {@inheritDoc} */
        @Override public boolean update(Object key, Object val, Object currVer, Object previousVer)
            throws CacheException {
            return stgy.update(key, val);
        }

        /** {@inheritDoc} */
        @Override public boolean afterUpdate(Object key, Object val, Object currVer, Object previousVer, SoftLock lock)
            throws CacheException {
            return stgy.afterUpdate(key, val, lock);
        }
    }
}