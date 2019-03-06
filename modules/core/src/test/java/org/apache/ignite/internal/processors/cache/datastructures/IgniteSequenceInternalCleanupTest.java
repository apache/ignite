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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode("client".equals(igniteInstanceName));

        cfg.setMetricsUpdateFrequency(10);

        cfg.setActiveOnStart(false);

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
    @Test
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
