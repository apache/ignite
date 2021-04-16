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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.transactions.TxDeadlockDetection.findCycle;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * DFS test for search cycle in wait-for-graph.
 */
public class DepthFirstSearchTest {
    /** Tx 1. */
    private static final GridCacheVersion T1 = new GridCacheVersion(1, 0, 0);

    /** Tx 2. */
    private static final GridCacheVersion T2 = new GridCacheVersion(2, 0, 0);

    /** Tx 3. */
    private static final GridCacheVersion T3 = new GridCacheVersion(3, 0, 0);

    /** Tx 4. */
    private static final GridCacheVersion T4 = new GridCacheVersion(4, 0, 0);

    /** Tx 5. */
    private static final GridCacheVersion T5 = new GridCacheVersion(5, 0, 0);

    /** Tx 6. */
    private static final GridCacheVersion T6 = new GridCacheVersion(6, 0, 0);

    /** All transactions. */
    private static final List<GridCacheVersion> ALL = Arrays.asList(T1, T2, T3, T4, T5, T6);


    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoCycle() throws Exception {
        assertNull(findCycle(Collections.<GridCacheVersion, Set<GridCacheVersion>>emptyMap(), T1));

        HashMap<GridCacheVersion, Set<GridCacheVersion>> wfg;

        wfg = new HashMap<GridCacheVersion, Set<GridCacheVersion>>() {{
            put(T1, null);
        }};

        assertAllNull(wfg);

        wfg = new HashMap<GridCacheVersion, Set<GridCacheVersion>>() {{
            put(T1, null);
            put(T2, null);
        }};

        assertAllNull(wfg);

        wfg = new HashMap<GridCacheVersion, Set<GridCacheVersion>>() {{
            put(T1, Collections.singleton(T2));
            put(T3, Collections.singleton(T4));
        }};

        assertAllNull(wfg);

        wfg = new HashMap<GridCacheVersion, Set<GridCacheVersion>>() {{
            put(T1, new HashSet<GridCacheVersion>() {{ add(T2); }});
            put(T2, new HashSet<GridCacheVersion>() {{ add(T3); }});
            put(T4, new HashSet<GridCacheVersion>() {{ add(T1); add(T3); }});
        }};

        assertAllNull(wfg);

        wfg = new HashMap<GridCacheVersion, Set<GridCacheVersion>>() {{
            put(T1, new HashSet<GridCacheVersion>() {{ add(T2); }});
            put(T2, new HashSet<GridCacheVersion>() {{ add(T3); }});
            put(T4, new HashSet<GridCacheVersion>() {{ add(T1); add(T2); add(T3); }});
        }};

        assertAllNull(wfg);

        wfg = new HashMap<GridCacheVersion, Set<GridCacheVersion>>() {{
            put(T1, new HashSet<GridCacheVersion>() {{ add(T2); }});
            put(T3, new HashSet<GridCacheVersion>() {{ add(T4); }});
            put(T4, new HashSet<GridCacheVersion>() {{ add(T1); }});
        }};

        assertAllNull(wfg);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFindCycle2() throws Exception {
        Map<GridCacheVersion, Set<GridCacheVersion>> wfg = new HashMap<GridCacheVersion, Set<GridCacheVersion>>() {{
            put(T1, Collections.singleton(T2));
            put(T2, Collections.singleton(T1));
        }};

        assertEquals(F.asList(T2, T1, T2), findCycle(wfg, T1));
        assertEquals(F.asList(T1, T2, T1), findCycle(wfg, T2));
        assertAllNull(wfg, T1, T2);

        wfg = new HashMap<GridCacheVersion, Set<GridCacheVersion>>() {{
            put(T1, Collections.singleton(T2));
            put(T2, Collections.singleton(T3));
            put(T3, asLinkedHashSet(T2, T4));
        }};

        assertEquals(F.asList(T3, T2, T3), findCycle(wfg, T1));
        assertEquals(F.asList(T3, T2, T3), findCycle(wfg, T2));
        assertEquals(F.asList(T2, T3, T2), findCycle(wfg, T3));
        assertAllNull(wfg, T1, T2, T3);

        wfg = new HashMap<GridCacheVersion, Set<GridCacheVersion>>() {{
            put(T1, Collections.singleton(T2));
            put(T2, asLinkedHashSet(T3, T1));
            put(T3, Collections.singleton(T2));
        }};

        assertEquals(F.asList(T2, T1, T2), findCycle(wfg, T1));
        assertEquals(F.asList(T1, T2, T1), findCycle(wfg, T2));
        assertEquals(F.asList(T2, T3, T2), findCycle(wfg, T3));
        assertAllNull(wfg, T1, T2, T3);

        wfg = new HashMap<GridCacheVersion, Set<GridCacheVersion>>() {{
            put(T1, Collections.singleton(T2));
            put(T2, asLinkedHashSet(T1, T3));
            put(T3, Collections.singleton(T4));
            put(T4, Collections.singleton(T3));
        }};

        assertEquals(F.asList(T2, T1, T2), findCycle(wfg, T1));
        assertEquals(F.asList(T4, T3, T4), findCycle(wfg, T2));
        assertEquals(F.asList(T4, T3, T4), findCycle(wfg, T3));
        assertEquals(F.asList(T3, T4, T3), findCycle(wfg, T4));
        assertAllNull(wfg, T1, T2, T3, T4);

        wfg = new HashMap<GridCacheVersion, Set<GridCacheVersion>>() {{
            put(T1, Collections.singleton(T2));
            put(T2, Collections.singleton(T3));
            put(T3, Collections.singleton(T4));
            put(T4, Collections.singleton(T5));
            put(T5, Collections.singleton(T6));
            put(T6, Collections.singleton(T5));
        }};

        assertEquals(F.asList(T6, T5, T6), findCycle(wfg, T1));
        assertEquals(F.asList(T6, T5, T6), findCycle(wfg, T2));
        assertEquals(F.asList(T6, T5, T6), findCycle(wfg, T3));
        assertEquals(F.asList(T6, T5, T6), findCycle(wfg, T4));
        assertEquals(F.asList(T6, T5, T6), findCycle(wfg, T5));
        assertEquals(F.asList(T5, T6, T5), findCycle(wfg, T6));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFindCycle3() throws Exception {
        Map<GridCacheVersion, Set<GridCacheVersion>> wfg = new HashMap<GridCacheVersion, Set<GridCacheVersion>>() {{
            put(T1, Collections.singleton(T2));
            put(T2, Collections.singleton(T3));
            put(T3, Collections.singleton(T1));
        }};

        assertEquals(F.asList(T3, T2, T1, T3), findCycle(wfg, T1));
        assertEquals(F.asList(T1, T3, T2, T1), findCycle(wfg, T2));
        assertEquals(F.asList(T2, T1, T3, T2), findCycle(wfg, T3));
        assertAllNull(wfg, T1, T2, T3);

        wfg = new HashMap<GridCacheVersion, Set<GridCacheVersion>>() {{
            put(T1, Collections.singleton(T2));
            put(T2, Collections.singleton(T3));
            put(T3, Collections.singleton(T4));
            put(T4, asLinkedHashSet(T2, T5));
        }};

        assertEquals(F.asList(T4, T3, T2, T4), findCycle(wfg, T1));
        assertEquals(F.asList(T4, T3, T2, T4), findCycle(wfg, T2));
        assertEquals(F.asList(T2, T4, T3, T2), findCycle(wfg, T3));
        assertEquals(F.asList(T3, T2, T4, T3), findCycle(wfg, T4));
        assertAllNull(wfg, T1, T2, T3, T4);

        wfg = new HashMap<GridCacheVersion, Set<GridCacheVersion>>() {{
            put(T1, Collections.singleton(T2));
            put(T2, asLinkedHashSet(T3, T4));
            put(T3, Collections.singleton(T1));
            put(T4, Collections.singleton(T5));
            put(T5, Collections.singleton(T6));
            put(T6, Collections.singleton(T4));

        }};

        assertEquals(F.asList(T6, T5, T4, T6), findCycle(wfg, T1));
        assertEquals(F.asList(T6, T5, T4, T6), findCycle(wfg, T2));
        assertEquals(F.asList(T2, T1, T3, T2), findCycle(wfg, T3));

        wfg = new HashMap<GridCacheVersion, Set<GridCacheVersion>>() {{
            put(T1, Collections.singleton(T2));
            put(T2, Collections.singleton(T3));
            put(T3, Collections.singleton(T4));
            put(T4, Collections.singleton(T5));
            put(T5, Collections.singleton(T6));
            put(T6, Collections.singleton(T4));
        }};

        assertEquals(F.asList(T6, T5, T4, T6), findCycle(wfg, T1));
        assertEquals(F.asList(T6, T5, T4, T6), findCycle(wfg, T2));
        assertEquals(F.asList(T6, T5, T4, T6), findCycle(wfg, T3));
        assertEquals(F.asList(T6, T5, T4, T6), findCycle(wfg, T4));
        assertEquals(F.asList(T4, T6, T5, T4), findCycle(wfg, T5));
        assertEquals(F.asList(T5, T4, T6, T5), findCycle(wfg, T6));

    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFindCycle4() throws Exception {
        Map<GridCacheVersion, Set<GridCacheVersion>> wfg = new HashMap<GridCacheVersion, Set<GridCacheVersion>>() {{
            put(T1, Collections.singleton(T2));
            put(T2, asLinkedHashSet(T3, T4));
            put(T3, Collections.singleton(T4));
            put(T4, Collections.singleton(T5));
            put(T6, Collections.singleton(T3));
        }};

        assertNull(findCycle(wfg, T1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRandomNoExceptions() throws Exception {
        int maxNodesCnt = 100;
        int minNodesCnt = 10;
        int maxWaitForNodesCnt = 20;

        int cyclesFound = 0;
        int cyclesNotFound = 0;

        Random seedRnd = new Random();

        Random rnd = new Random();

        for (int i = 0; i < 50000; i++) {
            long seed = seedRnd.nextLong();

            rnd.setSeed(seed);

            System.out.println(">>> Iteration " + i + " with seed " + seed);

            int nodesCnt = rnd.nextInt(maxNodesCnt - minNodesCnt) + minNodesCnt;

            Map<GridCacheVersion, Set<GridCacheVersion>> wfg = new HashMap<>();

            for (int j = 0; j < nodesCnt; j++) {
                if (rnd.nextInt(100) > 30) {
                    int waitForNodesCnt = rnd.nextInt(maxWaitForNodesCnt);

                    Set<GridCacheVersion> waitForNodes = null;

                    if (waitForNodesCnt > 0) {
                        waitForNodes = new LinkedHashSet<>();

                        for (int k = 0; k < waitForNodesCnt;) {
                            int n = rnd.nextInt(nodesCnt);

                            if (n != j) {
                                waitForNodes.add(new GridCacheVersion(n, 0, 0));
                                k++;
                            }
                        }
                    }

                    wfg.put(new GridCacheVersion(j, 0, 0), waitForNodes);
                }
            }

            for (int j = 0; j < nodesCnt; j++) {
                try {
                    List<GridCacheVersion> cycle = findCycle(wfg, new GridCacheVersion(j, 0, 0));

                    if (cycle == null)
                        cyclesNotFound++;
                    else
                        cyclesFound++;
                }
                catch (Throwable e) {
                    U.error(null, "Error during finding cycle in graph: ", e);

                    U.warn(null, "Seed: " + seed);

                    U.warn(null, "Wait-for-graph: " + wfg);

                    fail();
                }
            }
        }

        System.out.println(">>> Test finished. Cycles found: " + cyclesFound + ", cycles not found: " + cyclesNotFound);
    }

    /**
     * @param wfg Wait-for-graph.
     */
    private static void assertAllNull(Map<GridCacheVersion, Set<GridCacheVersion>> wfg, GridCacheVersion... ignore) {
        Set<GridCacheVersion> excl = F.asSet(ignore);

        for (GridCacheVersion tx : ALL) {
            if (!excl.contains(tx))
                assertNull(tx + " could not be part of cycle", findCycle(wfg, tx));
        }
    }

    /**
     * @param txs Txs.
     */
    private static Set<GridCacheVersion> asLinkedHashSet(GridCacheVersion... txs) {
        Set<GridCacheVersion> set = new LinkedHashSet<>();

        Collections.addAll(set, txs);

        return set;
    }
}
