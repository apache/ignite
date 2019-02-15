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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.io.Externalizable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheMetricsImpl;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMap;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTransactionalCache;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;

/**
 * DHT cache.
 */
public class GridDhtCache<K, V> extends GridDhtTransactionalCacheAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Near cache. */
    @GridToStringExclude
    private GridNearTransactionalCache<K, V> near;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtCache() {
        // No-op.
    }

    /**
     * @param ctx Context.
     */
    public GridDhtCache(GridCacheContext<K, V> ctx) {
        super(ctx);
    }

    /**
     * @param ctx Cache context.
     * @param map Cache map.
     */
    public GridDhtCache(GridCacheContext<K, V> ctx, GridCacheConcurrentMap map) {
        super(ctx, map);
    }

    /** {@inheritDoc} */
    @Override public boolean isDht() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        String name = super.name();

        return name == null ? "defaultDhtCache" : name + "Dht";
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        CacheMetricsImpl m = new CacheMetricsImpl(ctx);

        m.delegate(ctx.dht().near().metrics0());

        metrics = m;

        ctx.dr().resetMetrics();


        super.start();
    }

    /**
     * @return Near cache.
     */
    @Override public GridNearTransactionalCache<K, V> near() {
        return near;
    }

    /**
     * @param near Near cache.
     */
    public void near(GridNearTransactionalCache<K, V> near) {
        this.near = near;
    }
}