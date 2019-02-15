/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.cache.hibernate;

import java.util.Collections;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.Region;

/**
 * Implementation of {@link Region}. This interface defines base contract for all L2 cache regions.
 */
public class HibernateRegion implements Region {
    /** */
    protected final HibernateRegionFactory factory;

    /** */
    private final String name;

    /** Cache instance. */
    protected final HibernateCacheProxy cache;

    /** Grid instance. */
    protected Ignite ignite;

    /**
     * @param factory Region factory.
     * @param name Region name.
     * @param ignite Grid.
     * @param cache Region cache.
     */
    public HibernateRegion(HibernateRegionFactory factory, String name, Ignite ignite, HibernateCacheProxy cache) {
        this.factory = factory;
        this.name = name;
        this.ignite = ignite;
        this.cache = cache;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public void destroy() throws CacheException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object key) {
        return cache.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public long getSizeInMemory() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public long getElementCountInMemory() {
        return cache.size();
    }

    /** {@inheritDoc} */
    @Override public long getElementCountOnDisk() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public Map toMap() {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public long nextTimestamp() {
        return System.currentTimeMillis();
    }

    /** {@inheritDoc} */
    @Override public int getTimeout() {
        return 0;
    }
}