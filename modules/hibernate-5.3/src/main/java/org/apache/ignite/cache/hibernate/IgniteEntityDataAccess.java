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
import org.hibernate.cache.spi.DomainDataRegion;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;

/**
 * Ignite implementation of {@code EntityDataAccess} for Hibernate integration.
 */
public class IgniteEntityDataAccess extends IgniteCachedDomainDataAccess implements EntityDataAccess {
    /** */
    private final AccessType accessType;

    /** */
    public IgniteEntityDataAccess(HibernateAccessStrategyAdapter stgy, AccessType accessType,
        RegionFactory regionFactory,
        DomainDataRegion domainDataRegion, Ignite ignite,
        HibernateCacheProxy cache) {
        super(stgy, regionFactory, domainDataRegion, ignite, cache);

        this.accessType = accessType;
    }

    /** */
    @Override public Object generateCacheKey(Object id, EntityPersister persister, SessionFactoryImplementor factory,
        String tenantIdentifier) {
        return HibernateKeyWrapper.staticCreateEntityKey(id, persister, tenantIdentifier);
    }

    /** */
    @Override public Object getCacheKeyId(Object cacheKey) {
        return ((HibernateKeyWrapper) cacheKey).id();
    }

    /** */
    @Override public boolean insert(SharedSessionContractImplementor ses, Object key, Object val, Object ver) {
        return stgy.insert(key, val);
    }

    /** */
    @Override public boolean afterInsert(SharedSessionContractImplementor ses, Object key, Object val, Object ver) {
        return stgy.afterInsert(key, val);
    }

    /** */
    @Override public boolean update(SharedSessionContractImplementor ses, Object key, Object val, Object curVer, Object prevVer) {
        return stgy.update(key, val);
    }

    /** */
    @Override public boolean afterUpdate(SharedSessionContractImplementor ses, Object key, Object val, Object curVer, Object prevVer, SoftLock lock) {
        return stgy.afterUpdate(key, val);
    }

    /** */
    @Override public AccessType getAccessType() {
        return accessType;
    }
}
