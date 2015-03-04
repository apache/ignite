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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.hibernate.cache.*;
import org.hibernate.cache.spi.*;
import org.hibernate.cache.spi.access.*;

/**
 * Implementation of {@link NaturalIdRegion}. This region is used to store naturalId data.
 * <p>
 * L2 cache for entity naturalId and target cache region can be set using annotations:
 * <pre name="code" class="java">
 * &#064;javax.persistence.Entity
 * &#064;javax.persistence.Cacheable
 * &#064;org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
 * &#064;org.hibernate.annotations.NaturalIdCache
 * public class Entity {
 *     &#064;org.hibernate.annotations.NaturalId
 *     private String entityCode;
 *
 *     ...
 * }
 * </pre>
 */
public class GridHibernateNaturalIdRegion extends GridHibernateTransactionalDataRegion implements NaturalIdRegion {
    /**
     * @param factory Region factory.
     * @param name Region name.
     * @param ignite Grid.
     * @param cache Region cache,
     * @param dataDesc Region data description.
     */
    public GridHibernateNaturalIdRegion(GridHibernateRegionFactory factory, String name,
        Ignite ignite, GridCache<Object, Object> cache, CacheDataDescription dataDesc) {
        super(factory, name, ignite, cache, dataDesc);
    }

    /** {@inheritDoc} */
    @Override public NaturalIdRegionAccessStrategy buildAccessStrategy(AccessType accessType) throws CacheException {
        return new AccessStrategy(createAccessStrategy(accessType));
    }

    /**
     * NaturalId region access strategy.
     */
    private class AccessStrategy extends GridHibernateAbstractRegionAccessStrategy implements
        NaturalIdRegionAccessStrategy {
        /**
         * @param stgy Access strategy implementation.
         */
        private AccessStrategy(GridHibernateAccessStrategyAdapter stgy) {
            super(stgy);
        }

        /** {@inheritDoc} */
        @Override public NaturalIdRegion getRegion() {
            return GridHibernateNaturalIdRegion.this;
        }

        /** {@inheritDoc} */
        @Override public boolean insert(Object key, Object val) throws CacheException {
            return stgy.insert(key, val);
        }

        /** {@inheritDoc} */
        @Override public boolean afterInsert(Object key, Object val) throws CacheException {
            return stgy.afterInsert(key, val);
        }

        /** {@inheritDoc} */
        @Override public boolean update(Object key, Object val) throws CacheException {
            return stgy.update(key, val);
        }

        /** {@inheritDoc} */
        @Override public boolean afterUpdate(Object key, Object val, SoftLock lock) throws CacheException {
            return stgy.afterUpdate(key, val, lock);
        }
    }
}
