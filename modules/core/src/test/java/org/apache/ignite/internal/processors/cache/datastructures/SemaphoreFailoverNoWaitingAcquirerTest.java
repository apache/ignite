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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 * Class to test the retrieval of a permit on a semaphore after initial semaphore owner has been closed.
 *
 * IGNITE-7090
 *
 * <p>
 * <b><pre>
 * </pre></b>
 *
 */
public class SemaphoreFailoverNoWaitingAcquirerTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** Atomics cache mode. */
    private CacheMode atomicsCacheMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        AtomicConfiguration atomicCfg = atomicConfiguration();

        assertNotNull(atomicCfg);

        cfg.setAtomicConfiguration(atomicCfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReleasePermitsPartitioned() throws Exception {
        atomicsCacheMode = PARTITIONED;

        doTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReleasePermitsReplicated() throws Exception {
        atomicsCacheMode = REPLICATED;

        doTest();
    }

    /**
     * @throws Exception If failed.
     */
    private void doTest() throws Exception {
        try {
            startGrids(GRID_CNT);

            Ignite ignite = grid(0);

            IgniteSemaphore sem = ignite.semaphore("sem", 1, true, true);

            assertEquals(1, sem.availablePermits());

            sem.acquire(1);

            assertEquals(0, sem.availablePermits());

            ignite.close();

            awaitPartitionMapExchange();
            IgniteSemaphore sem2 = grid(1).semaphore("sem", 1, true, true);

            assertTrue("Could not aquire after 'restart'",sem2.tryAcquire(1, 5000, TimeUnit.MILLISECONDS));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @return Atomic configuration.
     */
    protected AtomicConfiguration atomicConfiguration() {
        AtomicConfiguration atomicCfg = new AtomicConfiguration();

        atomicCfg.setCacheMode(atomicsCacheMode);

        if (atomicsCacheMode == PARTITIONED)
            atomicCfg.setBackups(1);

        return atomicCfg;
    }
}
