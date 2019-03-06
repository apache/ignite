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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridTimer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.NONE;

/**
 * Partitioned affinity test.
 */
public class GridCachePartitionedAffinityExcludeNeighborsPerformanceTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRIDS = 3;

    /** Random number generator. */
    private static final Random RAND = new Random();

    /** */
    private boolean excNeighbores;

    /** */
    private static Collection<String> msgs = new ArrayList<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);

        cc.setBackups(2);

        AffinityFunction aff = new RendezvousAffinityFunction(excNeighbores);

        cc.setAffinity(aff);

        cc.setRebalanceMode(NONE);

        c.setCacheConfiguration(cc);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        msgs.clear();
    }

    /**
     * @param ignite Grid.
     * @return Affinity.
     */
    static Affinity<Object> affinity(Ignite ignite) {
        return ignite.affinity(DEFAULT_CACHE_NAME);
    }

    /**
     * @param aff Affinity.
     * @param key Key.
     * @return Nodes.
     */
    private static Collection<? extends ClusterNode> nodes(Affinity<Object> aff, Object key) {
        return aff.mapKeyToPrimaryAndBackups(key);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCountPerformance() throws Exception {
        excNeighbores = false;

        int cnt = 1000000;

        long t1 = checkCountPerformance(cnt, "includeNeighbors");

        System.gc();

        excNeighbores = true;

        long t2 = checkCountPerformance(cnt, "excludeNeighbors");

        for (String msg : msgs)
            info(msg);

        info(">>> t2/t1: " + (t2/t1));
    }

    /**
     * @param cnt Count.
     * @param testName Test name.
     * @return Duration.
     * @throws Exception If failed.
     */
    private long checkCountPerformance(int cnt, String testName) throws Exception {
        startGridsMultiThreaded(GRIDS);

        try {
            Ignite g = grid(0);

            // Warmup.
            checkCountPerformance0(g, 10000);

            info(">>> Starting count based test [testName=" + testName + ", cnt=" + cnt + ']');

            long dur = checkCountPerformance0(g, cnt);

            String msg = ">>> Performance [testName=" + testName + ", cnt=" + cnt + ", duration=" + dur + "ms]";

            info(">>> ");
            info(msg);
            info(">>> ");

            msgs.add(msg);

            return dur;
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     * @param g Grid.
     * @param cnt Count.
     * @return Result.
     * @throws Exception If failed.
     */
    private long checkCountPerformance0(Ignite g, int cnt) throws Exception {
        Affinity<Object> aff = affinity(g);

        GridTimer timer = new GridTimer("test");

        for (int i = 0; i < cnt; i++) {
            Object key = RAND.nextInt(Integer.MAX_VALUE);

            Collection<? extends ClusterNode> affNodes = nodes(aff, key);

            assert excNeighbores ? affNodes.size() == 1 : affNodes.size() == GRIDS;
        }

        timer.stop();

        return timer.duration();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTimedPerformance() throws Exception {
        excNeighbores = false;

        long dur = 15000;

        int cnt1 = checkTimedPerformance(dur, "includeNeighbors");

        System.gc();

        excNeighbores = true;

        int cnt2 = checkTimedPerformance(dur, "excludeNeighbors");

        for (String msg : msgs)
            info(msg);

        info(">>> cnt1/cnt2=" + (cnt1/cnt2));
    }

    /**
     * @param dur Duration.
     * @param testName Test name.
     * @return Number of operations.
     * @throws Exception If failed.
     */
    private int checkTimedPerformance(long dur, String testName) throws Exception {
        startGridsMultiThreaded(GRIDS);

        try {
            Ignite g = grid(0);

            Affinity<Object> aff = affinity(g);

            // Warmup.
            checkCountPerformance0(g, 10000);

            info(">>> Starting timed based test [testName=" + testName + ", duration=" + dur + ']');

            int cnt = 0;

            for (long t = System.currentTimeMillis(); cnt % 1000 != 0 || System.currentTimeMillis() - t < dur;) {
                Object key = RAND.nextInt(Integer.MAX_VALUE);

                Collection<? extends ClusterNode> affNodes = nodes(aff, key);

                assert excNeighbores ? affNodes.size() == 1 : affNodes.size() == GRIDS;

                cnt++;
            }

            String msg = ">>> Performance [testName=" + testName + ", duration=" + dur + "ms, cnt=" + cnt + ']';

            info(">>> ");
            info(msg);
            info(">>> ");

            msgs.add(msg);

            return cnt;
        }
        finally {
            stopAllGrids();
        }
    }
}
