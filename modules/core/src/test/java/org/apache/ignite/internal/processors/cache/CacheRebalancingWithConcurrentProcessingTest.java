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

package org.apache.ignite.internal.processors.cache;

import java.io.Externalizable;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/** */
public class CacheRebalancingWithConcurrentProcessingTest extends GridCommonAbstractTest {
    /** */
    private static final int NODE_COUNT = 4;

    /** */
    private IgniteCache cache;

    /** */
    private final TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setNearConfiguration(null);
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);

        for (int i = 0; i < NODE_COUNT; i++) {
            String nodeName = i == 0 ? "ignite" : "ignite" + i;

            Ignite node = startNode(nodeName, ipFinder);

            if (i == 0)
                cache = node.createCache(dataCacheCfg);
        }
    }

    /** */
    private Ignite startNode(String igniteInstanceName, TcpDiscoveryIpFinder ipFinder) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setLocalHost("127.0.0.1");
        cfg.setConnectorConfiguration(null);

        return G.start(cfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @SuppressWarnings("unchecked")
    public void testMultithreadedNoOpProcessingWhileRebalanceInProgress() throws Exception {
        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> fut = multithreadedAsync(() -> {
            while (!stop.get())
                cache.invoke(UUID.randomUUID(), new NoOpProcessor());
        }, 50);

        U.sleep(10000);

        stop.set(true);

        fut.get();
    }

    /**
     */
    @GridInternal
    private static final class NoOpProcessor implements EntryProcessor<UUID, byte[], Void>, Externalizable {
        /**
         * Empty constructor required for {@link Externalizable}.
         */
        @SuppressWarnings("WeakerAccess")
        public NoOpProcessor() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<UUID, byte[]> entry, Object... args) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) {
            // No-op.
        }
    }
}
