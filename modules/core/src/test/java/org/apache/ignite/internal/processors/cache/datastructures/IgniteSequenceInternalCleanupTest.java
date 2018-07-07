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

package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 */
public class IgniteSequenceInternalCleanupTest extends GridCommonAbstractTest {
    /** */
    public static final int GRIDS_CNT = 5;

    /** */
    public static final int SEQ_RESERVE = 50_000;

    /** */
    public static final int CACHES_CNT = 10;

    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode("client".equals(igniteInstanceName));

        cfg.setMetricsUpdateFrequency(10);

        cfg.setActiveOnStart(false);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        AtomicConfiguration atomicCfg = atomicConfiguration();

        assertNotNull(atomicCfg);

        cfg.setAtomicConfiguration(atomicCfg);

        List<CacheConfiguration> cacheCfg = new ArrayList<>();

        for (int i = 0; i < CACHES_CNT; i++) {
            cacheCfg.add(new CacheConfiguration("test" + i).
                setStatisticsEnabled(true).
                setCacheMode(PARTITIONED).
                setAtomicityMode(TRANSACTIONAL).
                setAffinity(new RendezvousAffinityFunction(false, 16)));
        }

        cfg.setCacheConfiguration(cacheCfg.toArray(new CacheConfiguration[cacheCfg.size()]));

        return cfg;
    }

    /** {@inheritDoc} */
    protected AtomicConfiguration atomicConfiguration() {
        AtomicConfiguration cfg = new AtomicConfiguration();

        cfg.setCacheMode(PARTITIONED);
        cfg.setBackups(1);
        cfg.setAtomicSequenceReserveSize(SEQ_RESERVE);

        return cfg;
    }

    /** */
    public void testDeactivate() throws Exception {
        try {
            Ignite ignite = startGridsMultiThreaded(GRIDS_CNT);

            ignite.cache("test0").put(0, 0);

            int id = 0;

            for (Ignite ig : G.allGrids()) {
                IgniteAtomicSequence seq = ig.atomicSequence("testSeq", 0, true);

                long id0 = seq.getAndIncrement();

                assertEquals(id0, id);

                id += SEQ_RESERVE;
            }

            doSleep(1000);

            long puts = ignite.cache("test0").metrics().getCachePuts();

            assertEquals(1, puts);

            grid(GRIDS_CNT - 1).cluster().active(false);

            ignite.cluster().active(true);

            long putsAfter = ignite.cache("test0").metrics().getCachePuts();

            assertEquals(0, putsAfter);
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }
}
