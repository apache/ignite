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
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.DomainDataRegion;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.access.CachedDomainDataAccess;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of L2 cache access strategy delegating to {@link HibernateAccessStrategyAdapter}.
 */
public abstract class IgniteCachedDomainDataAccess extends HibernateRegion implements CachedDomainDataAccess {
    /** */
    protected final HibernateAccessStrategyAdapter stgy;

    /** */
    private DomainDataRegion domainDataRegion;

    /**
     * @param stgy Access strategy implementation.
     */
    protected IgniteCachedDomainDataAccess(HibernateAccessStrategyAdapter stgy,
        RegionFactory regionFactory,
        DomainDataRegion domainDataRegion,
        Ignite ignite, HibernateCacheProxy cache) {
        super(regionFactory, cache.name(), ignite, cache);

        this.stgy = stgy;
        this.domainDataRegion = domainDataRegion;
    }

    /** {@inheritDoc} */
    @Override public DomainDataRegion getRegion() {
        return domainDataRegion;
    }

    /** {@inheritDoc} */
    @Override public Object get(SharedSessionContractImplementor ses, Object key) throws CacheException {
        return stgy.get(key);
    }

    /** {@inheritDoc} */
    @Override public boolean putFromLoad(SharedSessionContractImplementor ses, Object key, Object value, Object version) throws CacheException {
        stgy.putFromLoad(key, value);
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean putFromLoad(SharedSessionContractImplementor ses, Object key, Object value, Object version, boolean minimalPutOverride) throws CacheException {
        stgy.putFromLoad(key, value);
        return true;
    }

    /** {@inheritDoc} */
    @Override public SoftLock lockItem(SharedSessionContractImplementor ses, Object key, Object version) throws CacheException {
        stgy.lock(key);
        return null;
    }

    /** {@inheritDoc} */
    @Override public void unlockItem(SharedSessionContractImplementor ses, Object key, SoftLock lock) throws CacheException {
        stgy.unlock(key);
    }

    /** {@inheritDoc} */
    @Override public void remove(SharedSessionContractImplementor ses, Object key) throws CacheException {
        stgy.remove(key);
    }

    /** {@inheritDoc} */
    @Nullable @Override public SoftLock lockRegion() throws CacheException {
        stgy.lockRegion();

        return null;
    }

    /** {@inheritDoc} */
    @Override public void unlockRegion(SoftLock lock) throws CacheException {
        stgy.unlockRegion();
    }

    /** {@inheritDoc} */
    @Override public void removeAll(SharedSessionContractImplementor ses) throws CacheException {
        stgy.removeAll();
    }

    /** {@inheritDoc} */
    @Override public void evict(Object key) throws CacheException {
        stgy.evict(key);
    }

    /** {@inheritDoc} */
    @Override public void evictAll() throws CacheException {
        stgy.evictAll();
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object key) {
        return stgy.get(key) != null;
    }
}
