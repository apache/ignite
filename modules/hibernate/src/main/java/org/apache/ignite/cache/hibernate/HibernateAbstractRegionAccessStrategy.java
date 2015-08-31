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

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.RegionAccessStrategy;
import org.hibernate.cache.spi.access.SoftLock;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of L2 cache access strategy delegating to {@link HibernateAccessStrategyAdapter}.
 */
public abstract class HibernateAbstractRegionAccessStrategy implements RegionAccessStrategy {
    /** */
    protected final HibernateAccessStrategyAdapter stgy;

    /**
     * @param stgy Access strategy implementation.
     */
    protected HibernateAbstractRegionAccessStrategy(HibernateAccessStrategyAdapter stgy) {
        this.stgy = stgy;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object get(Object key, long txTs) throws CacheException {
        return stgy.get(key);
    }

    /** {@inheritDoc} */
    @Override public boolean putFromLoad(Object key, Object val, long txTs, Object ver) throws CacheException {
        stgy.putFromLoad(key, val);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean putFromLoad(Object key, Object val, long txTs, Object ver, boolean minimalPutOverride)
        throws CacheException {
        stgy.putFromLoad(key, val, minimalPutOverride);

        return true;
    }

    /** {@inheritDoc} */
    @Nullable @Override public SoftLock lockItem(Object key, Object ver) throws CacheException {
        return stgy.lock(key);
    }

    /** {@inheritDoc} */
    @Nullable @Override public SoftLock lockRegion() throws CacheException {
        return stgy.lockRegion();
    }

    /** {@inheritDoc} */
    @Override public void unlockRegion(SoftLock lock) throws CacheException {
        stgy.unlockRegion(lock);
    }

    /** {@inheritDoc} */
    @Override public void unlockItem(Object key, SoftLock lock) throws CacheException {
        stgy.unlock(key, lock);
    }

    /** {@inheritDoc} */
    @Override public void remove(Object key) throws CacheException {
        stgy.remove(key);
    }

    /** {@inheritDoc} */
    @Override public void removeAll() throws CacheException {
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
}