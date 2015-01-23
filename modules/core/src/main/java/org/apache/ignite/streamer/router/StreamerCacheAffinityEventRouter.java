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

package org.apache.ignite.streamer.router;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.streamer.*;
import org.jetbrains.annotations.*;

/**
 * Router used to colocate streamer events with data stored in a partitioned cache.
 * <h1 class="header">Affinity Key</h1>
 * Affinity key for collocation of event together on the same node is specified
 * via {@link CacheAffinityEvent#affinityKey()} method. If event does not implement
 * {@link CacheAffinityEvent} interface, then event will be routed always to local node.
 */
public class StreamerCacheAffinityEventRouter extends StreamerEventRouterAdapter {
    /**
     * All events that implement this interface will be routed based on key affinity.
     */
    @SuppressWarnings("PublicInnerClass")
    public interface CacheAffinityEvent {
        /**
         * @return Affinity route key for the event.
         */
        public Object affinityKey();

        /**
         * @return Cache name, if {@code null}, the default cache is used.
         */
        @Nullable public String cacheName();
    }

    /** Grid instance. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public <T> ClusterNode route(StreamerContext ctx, String stageName, T evt) {
        if (evt instanceof CacheAffinityEvent) {
            CacheAffinityEvent e = (CacheAffinityEvent)evt;

            GridCache<Object, Object> c = ((GridEx) ignite).cachex(e.cacheName());

            assert c != null;

            return c.affinity().mapKeyToNode(e.affinityKey());
        }

        return ignite.cluster().localNode();
    }
}
