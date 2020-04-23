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
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.ExtendedStatisticsSupport;
import org.hibernate.cache.spi.Region;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.support.AbstractRegion;

/**
 * Implementation of {@link Region}. This interface defines base contract for all L2 cache regions.
 */
public abstract class HibernateRegion extends AbstractRegion implements ExtendedStatisticsSupport {
    /** Cache instance. */
    protected final HibernateCacheProxy cache;

    /** Grid instance. */
    protected Ignite ignite;

    /** */
    protected HibernateRegion(RegionFactory factory, String name, Ignite ignite, HibernateCacheProxy cache) {
        super(name, factory);

        this.ignite = ignite;
        this.cache = cache;
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        try {
            cache.clear();
        } catch (IgniteCheckedException e) {
            throw new CacheException("Problem clearing cache [name=" + cache.name() + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void destroy() throws CacheException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public long getElementCountInMemory() {
        return cache.offHeapEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long getElementCountOnDisk() {
        return cache.sizeLong();
    }

    /** {@inheritDoc} */
    @Override public long getSizeInMemory() {
        return cache.offHeapAllocatedSize();
    }
}
