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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterGroupEmptyException;
import org.apache.ignite.configuration.CacheConfiguration;

import java.io.Serializable;
import java.net.NetworkInterface;
import java.util.*;
import java.util.concurrent.locks.Lock;

/**
 * Cluster port manager that allows to reliably {@link #acquirePort(UUID)} and {@link #releasePort(UUID, int)} on the
 * cluster nodes.
 */
public class ClusterPortManager {
    /** Ignite instance. */
    private final Ignite ignite;

    /** Ignite logger. */
    private final IgniteLogger log;

    /** Port manager cache name. */
    private final String portMgrCacheName;

    /** Port range from point. */
    private final int from;

    /** Port range size. */
    private final int cnt;

    /** Port manager cache */
    private final IgniteCache<HostIdentifier, BitSet> cache;

    /**
     * Constructs a new instance of cluster port manager.
     *
     * @param poolName Port pool name.
     * @param from Port range from point.
     * @param cnt Port range size.
     */
    public ClusterPortManager(Ignite ignite, String poolName, int from, int cnt) {
        assert ignite != null : "Ignite instance should not be null";
        assert poolName != null : "Pool name should not be null";
        assert cnt >= 0 : "Count should not be negative";
        assert from >= 0 && cnt + from <= 0xFFFF : "Port range should be between 0 and 65535";

        this.ignite = ignite;
        this.log = ignite.log().getLogger(ClusterPortManager.class);

        this.portMgrCacheName = String.format("PORT_MANAGER_%s_CACHE", poolName);
        this.from = from;
        this.cnt = cnt;
        this.cache = getOrCreateCache();
    }

    /**
     * Acquires free port on the specified node.
     *
     * @param nodeId Node identifier.
     * @return Port to be acquired.
     */
    public int acquirePort(UUID nodeId) {
        HostIdentifier hostId = getHostIdentifier(nodeId);

        if (hostId == null)
            throw new IllegalStateException("Can't find node [nodeId=" + nodeId + "]");

        Lock lock = cache.lock(hostId);
        lock.lock();

        try {
            BitSet ports = cache.get(hostId);

            if (ports == null)
                ports = new BitSet(cnt);

            int free = ports.nextClearBit(0);

            if (free >= cnt)
                throw new IllegalStateException("No free ports in range [from=" + from + ", cnt=" + cnt + "]");

            ports.set(free);
            log.debug("Port acquired [nodeId=" + nodeId + ", port=" + (from + free) + "]");

            cache.put(hostId, ports);

            return from + free;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Releases acquired port on the specified node.
     *
     * @param nodeId Node identifier.
     * @param port Acquired port to be free.
     */
    public void releasePort(UUID nodeId, int port) {
        assert port - from >= 0 && port - from < cnt : "Port not in the range";

        HostIdentifier hostId = getHostIdentifier(nodeId);

        if (hostId == null)
            return;

        Lock lock = cache.lock(hostId);
        lock.lock();

        try {
            BitSet ports = cache.get(hostId);

            if (ports != null) {
                ports.clear(port - from);
                log.debug("Port released [nodeId=" + nodeId + ", port=" + port + "]");

                if (ports.isEmpty())
                    cache.remove(hostId);
                else
                    cache.put(hostId, ports);
            }
        }
        finally {
            lock.unlock();
        }
    }

    /** Destroys port manager and related caches. */
    public void destroy() {
        ignite.destroyCache(portMgrCacheName);
    }

    /**
     * Returns existed port pool cache or creates a new one.
     *
     * @return Port pool cache.
     */
    private IgniteCache<HostIdentifier, BitSet> getOrCreateCache() {
        CacheConfiguration<HostIdentifier, BitSet> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName(portMgrCacheName);
        cacheConfiguration.setCacheMode(CacheMode.REPLICATED);
        cacheConfiguration.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        return ignite.getOrCreateCache(cacheConfiguration);
    }

    /**
     * Returns host identifier by node identifier.
     *
     * @param nodeId Node identifier.
     * @return Host identifier.
     */
    private HostIdentifier getHostIdentifier(UUID nodeId) {
        try {
            ClusterGroup grp = ignite.cluster().forNodeId(nodeId);

            return ignite.compute(grp).call(() -> {
                Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();

                List<byte[]> macAddrs = new ArrayList<>();

                while (interfaces.hasMoreElements()) {
                    NetworkInterface netItf = interfaces.nextElement();
                    byte[] macAddr = netItf.getHardwareAddress();
                    macAddrs.add(macAddr);
                }

                return new HostIdentifier(macAddrs.toArray(new byte[macAddrs.size()][]));
            });
        }
        catch (ClusterGroupEmptyException e) {
            return null;
        }
    }

    /**
     * Host identifier based on arrays of mac addresses of the host machine.
     */
    private static class HostIdentifier implements Serializable {
        /** */
        private static final long serialVersionUID = -7060231325908935162L;

        /** Mac addresses. */
        private final byte[][] macAddrs;

        /**
         * Constructs a new instance of host identifier.
         *
         * @param macAddrs Mac addresses.
         */
        public HostIdentifier(byte[][] macAddrs) {
            this.macAddrs = macAddrs;
        }

        /** */
        public byte[][] getMacAddrs() {
            return macAddrs;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            HostIdentifier that = (HostIdentifier)o;
            if (macAddrs.length != that.macAddrs.length)
                return false;

            for (int i = 0; i < macAddrs.length; i++)
                if (!Arrays.equals(macAddrs[i], that.macAddrs[i]))
                    return false;

            return true;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Arrays.hashCode(macAddrs);
        }
    }
}
