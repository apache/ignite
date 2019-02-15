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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;

/**
 * Implementation of READ_ONLY cache access strategy.
 * <p>
 * Configuration of L2 cache and per-entity cache access strategy can be set in the
 * Hibernate configuration file:
 * <pre name="code" class="xml">
 * &lt;hibernate-configuration&gt;
 *     &lt;!-- Enable L2 cache. --&gt;
 *     &lt;property name="cache.use_second_level_cache"&gt;true&lt;/property&gt;
 *
 *     &lt;!-- Use Ignite as L2 cache provider. --&gt;
 *     &lt;property name="cache.region.factory_class"&gt;org.apache.ignite.cache.hibernate.HibernateRegionFactory&lt;/property&gt;
 *
 *     &lt;!-- Specify entity. --&gt;
 *     &lt;mapping class="com.example.Entity"/&gt;
 *
 *     &lt;!-- Enable L2 cache with read-only access strategy for entity. --&gt;
 *     &lt;class-cache class="com.example.Entity" usage="read-only"/&gt;
 * &lt;/hibernate-configuration&gt;
 * </pre>
 * Also cache access strategy can be set using annotations:
 * <pre name="code" class="java">
 * &#064;javax.persistence.Entity
 * &#064;javax.persistence.Cacheable
 * &#064;org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.READ_ONLY)
 * public class Entity { ... }
 * </pre>
 *
 */
public class HibernateReadOnlyAccessStrategy extends HibernateAccessStrategyAdapter {
    /**
     * @param ignite Node.
     * @param cache Cache.
     * @param eConverter Exception converter.
     */
    public HibernateReadOnlyAccessStrategy(
        Ignite ignite,
        HibernateCacheProxy cache,
        HibernateExceptionConverter eConverter) {
        super(ignite, cache, eConverter);
    }

    /** {@inheritDoc} */
    @Override public boolean insert(Object key, Object val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean afterInsert(Object key, Object val) {
        if (log.isDebugEnabled())
            log.debug("Put [cache=" + cache.name() + ", key=" + key + ']');

        try {
            cache.put(key, val);

            return true;
        }
        catch (IgniteCheckedException e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void lock(Object key) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void unlock(Object key) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void remove(Object key) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean update(Object key, Object val) {
        throw new UnsupportedOperationException("Updates are not supported for read-only access strategy.");
    }

    /** {@inheritDoc} */
    @Override public boolean afterUpdate(Object key, Object val) {
        throw new UnsupportedOperationException("Updates are not supported for read-only access strategy.");
    }
}
