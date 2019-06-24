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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Test correctness of rollback a transaction with timeout during the grid stop.
 */
public class IgniteTxRemoveTimeoutObjectsTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int PUT_CNT = 1000;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 60_000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Assume.assumeFalse("https://issues.apache.org/jira/browse/IGNITE-7388", MvccFeatureChecker.forcedMvcc());

        if (nearEnabled())
            MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.NEAR_CACHE);

        super.beforeTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxRemoveTimeoutObjects() throws Exception {
        IgniteCache<Integer, Integer> cache0 = grid(0).cache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, Integer> cache1 = grid(1).cache(DEFAULT_CACHE_NAME);

        // start additional grid to be closed.
        IgniteCache<Integer, Integer> cacheAdditional = startGrid(gridCount()).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < PUT_CNT; i++)
            cache0.put(i, Integer.MAX_VALUE);

        logTimeoutObjectsFrequency();

        info("Tx1 started");
        try (Transaction tx = grid(gridCount()).transactions().txStart(PESSIMISTIC, SERIALIZABLE, 100, PUT_CNT)) {
            try {
                for (int i = 0; i < PUT_CNT; i++) {
                    cacheAdditional.put(i, Integer.MIN_VALUE);

                    if (i % 100 == 0)
                        logTimeoutObjectsFrequency();
                }

                U.sleep(200);

                tx.commit();

                fail("A timeout should have happened.");
            }
            catch (Exception e) {
                assertTrue(X.hasCause(e, TransactionTimeoutException.class));
            }
        }

        assertDoesNotContainLockTimeoutObjects();

        logTimeoutObjectsFrequency();

        stopGrid(gridCount());

        awaitPartitionMapExchange();

        info("Grid2 closed.");

        assertDoesNotContainLockTimeoutObjects();

        logTimeoutObjectsFrequency();

        // Check that the values have not changed and lock can be acquired.
        try (Transaction tx2 = grid(1).transactions().txStart(PESSIMISTIC, SERIALIZABLE)) {
            info("Tx2 started");

            for (int i = 0; i < PUT_CNT; i++) {
                assertEquals(cache1.get(i).intValue(), Integer.MAX_VALUE);
                cache1.put(i, i);

                if (i % (PUT_CNT / 5) == 0)
                    logTimeoutObjectsFrequency();
            }

            tx2.commit();
        }

        info("Tx2 stopped");

        // Assertions should be into a transaction because of near cache.
        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, SERIALIZABLE)) {

            // Check that changes committed.
            for (int i = 0; i < PUT_CNT; i++)
                assertEquals(cache0.get(i).intValue(), i);

            tx.commit();
        }
    }

    /**
     * Fails if at least one grid contains LockTimeoutObjects.
     */
    private void assertDoesNotContainLockTimeoutObjects() throws IgniteInterruptedCheckedException {
        boolean noLockTimeoutObjs = GridTestUtils.waitForCondition(() -> {
            for (Ignite ignite : G.allGrids()) {
                for (GridTimeoutObject object : getTimeoutObjects((IgniteEx)ignite)) {
                    if (object.getClass().getSimpleName().equals("LockTimeoutObject"))
                        return false;
                }
            }

            return true;
        }, getTestTimeout());

        if (!noLockTimeoutObjs)
            fail("Grids contain LockTimeoutObjects.");
    }

    /**
     * Print the number of each timeout object type on each grid to the log.
     */
    private void logTimeoutObjectsFrequency() {
        StringBuilder sb = new StringBuilder("Timeout objects frequency [");

        for (Ignite ignite : G.allGrids()) {
            IgniteEx igniteEx = (IgniteEx)ignite;

            Map<String, Integer> objFreqMap = new HashMap<>();

            Set<GridTimeoutObject> objs = getTimeoutObjects(igniteEx);

            for (GridTimeoutObject obj : objs) {
                String clsName = obj.getClass().getSimpleName();

                Integer cnt = objFreqMap.get(clsName);

                if (cnt == null)
                    objFreqMap.put(clsName, 1);
                else
                    objFreqMap.put(clsName, cnt + 1);
            }

            sb.append("[")
                .append(igniteEx.name()).append(": size=")
                .append(objs.size()).append(", ");

            for (Map.Entry<String, Integer> entry : objFreqMap.entrySet()) {
                sb.append(entry.getKey()).append("=")
                    .append(entry.getValue())
                    .append(", ");
            }

            sb.delete(sb.length() - 2, sb.length())
                .append("]; ");
        }

        sb.delete(sb.length() - 2, sb.length())
            .append("]");

        info(sb.toString()
            .replaceAll("distributed.IgniteTxRemoveTimeoutObjectsTest", "Grid"));
    }

    /**
     * @param igniteEx IgniteEx.
     * @return Set of timeout objects that process on current IgniteEx.
     */
    private Set<GridTimeoutObject> getTimeoutObjects(IgniteEx igniteEx) {
        GridTimeoutProcessor timeout = igniteEx.context().timeout();

        return GridTestUtils.getFieldValue(timeout, timeout.getClass(), "timeoutObjs");
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }
}
