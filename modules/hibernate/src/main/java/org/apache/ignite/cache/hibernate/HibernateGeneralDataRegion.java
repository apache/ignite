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
import org.hibernate.cache.spi.GeneralDataRegion;
import org.hibernate.cache.spi.QueryResultsRegion;
import org.hibernate.cache.spi.TimestampsRegion;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link GeneralDataRegion}. This interface defines common contract for {@link QueryResultsRegion}
 * and {@link TimestampsRegion}.
 */
public class HibernateGeneralDataRegion extends HibernateRegion implements GeneralDataRegion {
    /**
     * @param factory Region factory.
     * @param name Region name.
     * @param ignite Grid.
     * @param cache Region cache.
     */
    public HibernateGeneralDataRegion(HibernateRegionFactory factory, String name,
        Ignite ignite, IgniteInternalCache<Object, Object> cache) {
        super(factory, name, ignite, cache);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object get(Object key) throws CacheException {
        try {
            return cache.get(key);
        } catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(Object key, Object val) throws CacheException {
        try {
            cache.put(key, val);
        } catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void evict(Object key) throws CacheException {
        HibernateAccessStrategyAdapter.evict(ignite, cache, key);
    }

    /** {@inheritDoc} */
    @Override public void evictAll() throws CacheException {
        HibernateAccessStrategyAdapter.evictAll(cache);
    }
}