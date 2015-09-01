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
import org.hibernate.cache.spi.CollectionRegion;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.CollectionRegionAccessStrategy;

/**
 * Implementation of {@link CollectionRegion}. This region is used to store collection data.
 * <p>
 * L2 cache for collection can be enabled in the Hibernate configuration file:
 * <pre name="code" class="xml">
 * &lt;hibernate-configuration&gt;
 *     &lt;!-- Enable L2 cache. --&gt;
 *     &lt;property name="cache.use_second_level_cache"&gt;true&lt;/property&gt;
 *
 *     &lt;!-- Use Ignite as L2 cache provider. --&gt;
 *     &lt;property name="cache.region.factory_class"&gt;org.apache.ignite.cache.hibernate.HibernateRegionFactory&lt;/property&gt;
 *
 *     &lt;!-- Specify entities. --&gt;
 *     &lt;mapping class="com.example.Entity"/&gt;
 *     &lt;mapping class="com.example.ChildEntity"/&gt;
 *
 *     &lt;!-- Enable L2 cache with nonstrict-read-write access strategy for entities and collection. --&gt;
 *     &lt;collection-cache collection="com.example.Entity" usage="nonstrict-read-write"/&gt;
 *     &lt;collection-cache collection="com.example.ChildEntity" usage="nonstrict-read-write"/&gt;
 *     &lt;collection-cache collection="com.example.Entity.children" usage="nonstrict-read-write"/&gt;
 * &lt;/hibernate-configuration&gt;
 * </pre>
 * Also cache for collection can be enabled using annotations:
 * <pre name="code" class="java">
 * &#064;javax.persistence.Entity
 * public class Entity {
 *    ...
 *
 *    &#064;javax.persistence.OneToMany(cascade=CascadeType.ALL, fetch=FetchType.EAGER)
 *    &#064;javax.persistence.JoinColumn(name="PARENT_ID")
 *    &#064;org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
 *    public List&lt;ChildEntity&gt; getChildren() {...}
 * }
 * </pre>
 * Note: the collection cache does not cache the state of the actual entities in the cache, it caches only identifier
 * values. For this reason, the collection cache should always be used in conjunction with
 * the second-level cache for those entities expected to be cached as part of a collection cache.
 */
public class HibernateCollectionRegion extends HibernateTransactionalDataRegion implements CollectionRegion {
    /**
     * @param factory Region factory.
     * @param name Region name.
     * @param ignite Grid.
     * @param cache Region cache.
     * @param dataDesc Region data description.
     */
    public HibernateCollectionRegion(HibernateRegionFactory factory, String name,
        Ignite ignite, IgniteInternalCache<Object, Object> cache, CacheDataDescription dataDesc) {
        super(factory, name, ignite, cache, dataDesc);
    }

    /** {@inheritDoc} */
    @Override public CollectionRegionAccessStrategy buildAccessStrategy(AccessType accessType) throws CacheException {
        return new AccessStrategy(createAccessStrategy(accessType));
    }

    /**
     * Collection region access strategy.
     */
    private class AccessStrategy extends HibernateAbstractRegionAccessStrategy
        implements CollectionRegionAccessStrategy {
        /**
         * @param stgy Access strategy implementation.
         */
        private AccessStrategy(HibernateAccessStrategyAdapter stgy) {
            super(stgy);
        }

        /** {@inheritDoc} */
        @Override public CollectionRegion getRegion() {
            return HibernateCollectionRegion.this;
        }
    }
}