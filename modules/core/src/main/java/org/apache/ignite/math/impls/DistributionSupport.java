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

package org.apache.ignite.math.impls;

import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.lang.*;
import javax.cache.*;
import java.util.*;
import java.util.function.*;

/**
 * Distribution-related misc. support.
 */
public class DistributionSupport {
    /**
     * Gets local Ignite instance.
     */
    protected Ignite ignite() {
        return Ignition.localIgnite();
    }

    /**
     *
     * @param cacheName
     * @param run
     */
    protected void broadcastForCache(String cacheName, IgniteRunnable run) {
        ignite().compute(ignite().cluster().forCacheNodes(cacheName)).broadcast(run);
    }

    /**
     * 
     * @param cacheName
     * @param clo
     * @param <K>
     * @param <V>
     */
    protected <K, V> void iterateOverEntries(
        String cacheName,
        BiConsumer<Cache.Entry<K, V>, IgniteCache<K, V>> clo) {
        int partsCnt = ignite().affinity(cacheName).partitions();

        broadcastForCache(cacheName, () -> {
            IgniteCache<K, V> cache = Ignition.localIgnite().getOrCreateCache(cacheName);

            // Iterate over all partitions. Some of them will be stored on that local node.
            for (int part = 0; part < partsCnt; part++)
                // Iterate over given partition.
                // Query returns an empty cursor if this partition is not stored on this node.
                for (Cache.Entry<K, V> entry : cache.query(new ScanQuery<K, V>(part))) 
                    clo.accept(entry, cache);
        });
    }

    /**
     * 
     * @param cacheName
     * @param call
     * @param <A>
     * @return
     */
    protected <A> Collection<A> broadcastForCache(String cacheName, IgniteCallable<A> call) {
        return ignite().compute(ignite().cluster().forCacheNodes(cacheName)).broadcast(call);
    }
}
