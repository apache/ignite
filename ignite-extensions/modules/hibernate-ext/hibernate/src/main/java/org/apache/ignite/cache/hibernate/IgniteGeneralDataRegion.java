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
import org.apache.ignite.IgniteLogger;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.DirectAccessRegion;
import org.hibernate.cache.spi.QueryResultsRegion;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.TimestampsRegion;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link DirectAccessRegion}. This interface defines common contract for {@link QueryResultsRegion}
 * and {@link TimestampsRegion}.
 */
public class IgniteGeneralDataRegion extends HibernateRegion implements DirectAccessRegion {
    /** */
    private final IgniteLogger log;

    /**
     * @param factory Region factory.
     * @param name Region name.
     * @param ignite Grid.
     * @param cache Region cache.
     */
    IgniteGeneralDataRegion(RegionFactory factory, String name,
        Ignite ignite, HibernateCacheProxy cache) {
        super(factory, name, ignite, cache);

        log = ignite.log().getLogger(getClass());
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object getFromCache(Object key, SharedSessionContractImplementor ses) throws CacheException {
        try {
            Object val = cache.get(key);

            if (log.isDebugEnabled())
                log.debug("Get [cache=" + cache.name() + ", key=" + key + ", val=" + val + ']');

            return val;
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void putIntoCache(Object key, Object val, SharedSessionContractImplementor ses) throws CacheException {
        try {
            cache.put(key, val);

            if (log.isDebugEnabled())
                log.debug("Put [cache=" + cache.name() + ", key=" + key + ", val=" + val + ']');
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        try {
            cache.clear();
        }
        catch (IgniteCheckedException e) {
            throw new CacheException("Problem clearing cache [name=" + cache.name() + "]", e);
        }
    }
}
