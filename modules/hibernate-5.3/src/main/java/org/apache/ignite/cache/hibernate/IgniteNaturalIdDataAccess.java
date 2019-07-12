/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.hibernate;

import org.apache.ignite.Ignite;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.internal.DefaultCacheKeysFactory;
import org.hibernate.cache.spi.DomainDataRegion;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.NaturalIdDataAccess;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;

/**
 * Ignite implementation of {@code NaturalIdDataAccess} for Hibernate integration.
 */
public class IgniteNaturalIdDataAccess extends IgniteCachedDomainDataAccess implements NaturalIdDataAccess {
    /** */
    private final AccessType accessType;

    /** */
    public IgniteNaturalIdDataAccess(HibernateAccessStrategyAdapter stgy, AccessType accessType,
        RegionFactory regionFactory,
        DomainDataRegion domainDataRegion, Ignite ignite,
        HibernateCacheProxy cache) {
        super(stgy, regionFactory, domainDataRegion, ignite, cache);

        this.accessType = accessType;
    }

    /** */
    @Override public AccessType getAccessType() {
        return accessType;
    }

    /** */
    @Override public Object generateCacheKey(Object[] naturalIdValues, EntityPersister persister, SharedSessionContractImplementor ses) {
        return DefaultCacheKeysFactory.staticCreateNaturalIdKey(naturalIdValues, persister, ses);
    }

    /** */
    @Override public Object[] getNaturalIdValues(Object cacheKey) {
        return DefaultCacheKeysFactory.staticGetNaturalIdValues(cacheKey);
    }

    /** */
    @Override public boolean insert(SharedSessionContractImplementor ses, Object key, Object val) throws CacheException {
        return stgy.insert(key, val);
    }

    /** */
    @Override public boolean afterInsert(SharedSessionContractImplementor ses, Object key, Object val) throws CacheException {
        return stgy.afterInsert(key, val);
    }

    /** */
    @Override public boolean update(SharedSessionContractImplementor ses, Object key, Object val) throws CacheException {
        return stgy.update(key, val);
    }

    /** */
    @Override public boolean afterUpdate(SharedSessionContractImplementor ses, Object key, Object val, SoftLock lock) throws CacheException {
        return stgy.afterUpdate(key, val);
    }
}
