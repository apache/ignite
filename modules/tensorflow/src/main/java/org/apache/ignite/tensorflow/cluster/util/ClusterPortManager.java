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

package org.apache.ignite.tensorflow.cluster.util;

import java.io.Serializable;
import java.util.BitSet;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Cluster port manager that allows to reliably {@link #acquirePort(UUID)} and {@link #freePort(UUID, int)} on the
 * cluster nodes.
 */
public class ClusterPortManager implements Serializable {
    /** */
    private static final long serialVersionUID = -5116593574559007292L;

    /** Port manager cache name. */
    private final String portMgrCacheName;

    /** Port range from point. */
    private final int from;

    /** Port range size. */
    private final int cnt;

    /** Ignite instance supplier. */
    private final Supplier<Ignite> igniteSupplier;

    /** Port manager cache */
    private transient IgniteCache<UUID, BitSet> cache;

    /**
     * Constructs a new instance of cluster port manager.
     *
     * @param poolName Port pool name.
     * @param from Port range from point.
     * @param cnt Port range size.
     */
    public <T extends Supplier<Ignite> & Serializable> ClusterPortManager(String poolName, int from, int cnt,
        T igniteSupplier) {
        assert poolName != null : "Pool name should not be null";
        assert cnt >= 0 : "Count should not be negative";
        assert from >= 0 && cnt + from <= 0xFFFF : "Port range should be between 0 and 65535";
        assert igniteSupplier != null : "Ignite supplier should not be null";

        this.portMgrCacheName = String.format("PORT_MANAGER_CACHE_%s", poolName);
        this.from = from;
        this.cnt = cnt;
        this.igniteSupplier = igniteSupplier;
    }

    /** Initializes port manager and creates or gets correspondent caches. */
    public void init() {
        CacheConfiguration<UUID, BitSet> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName(portMgrCacheName);
        cacheConfiguration.setCacheMode(CacheMode.REPLICATED);
        cacheConfiguration.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        Ignite ignite = igniteSupplier.get();
        cache = ignite.getOrCreateCache(cacheConfiguration);
    }

    /**
     * Acquires free port on the specified node.
     *
     * @param nodeId Node identifier.
     * @return Port to be acquired.
     */
    public int acquirePort(UUID nodeId) {
        checkThatInitialized();

        Lock lock = cache.lock(nodeId);
        lock.lock();

        try {
            BitSet ports = cache.get(nodeId);

            if (ports == null)
                ports = new BitSet(cnt);

            int free = ports.nextClearBit(0);

            if (free >= cnt)
                throw new IllegalStateException("No free ports in range [from=" + from + ", cnt=" + cnt + "]");

            ports.set(free);

            cache.put(nodeId, ports);

            return from + free;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Frees acquired port on the specified node.
     *
     * @param nodeId Node identifier.
     * @param port Acquired port to be free.
     */
    public void freePort(UUID nodeId, int port) {
        assert port - from >= 0 && port - from < cnt : "Port not in the range";

        checkThatInitialized();

        Lock lock = cache.lock(nodeId);
        lock.lock();

        try {
            BitSet ports = cache.get(nodeId);

            if (ports != null) {
                ports.clear(port - from);

                if (ports.isEmpty())
                    cache.remove(nodeId);
            }
        }
        finally {
            lock.unlock();
        }
    }

    /** Destroys port manager and related caches. */
    public void destroy() {
        Ignite ignite = igniteSupplier.get();
        ignite.destroyCache(portMgrCacheName);
    }

    /**
     * Checks that the component has been initialized.
     */
    private void checkThatInitialized() {
        if (cache == null)
            throw new IllegalStateException("Cluster Port Manager is not initialized");
    }
}
