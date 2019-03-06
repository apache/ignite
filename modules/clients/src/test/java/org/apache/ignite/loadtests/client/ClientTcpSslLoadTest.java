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

package org.apache.ignite.loadtests.client;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.client.ClientTcpSslMultiThreadedSelfTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

/**
 * Makes a long run to ensure stability and absence of memory leaks.
 */
public class ClientTcpSslLoadTest extends ClientTcpSslMultiThreadedSelfTest {
    /** Test duration. */
    private static final long TEST_RUN_TIME = 8 * 60 * 60 * 1000;

    /** Statistics output interval. */
    private static final long STATISTICS_PRINT_INTERVAL = 5 * 60 * 1000;

    /** Time to let connections closed by idle. */
    private static final long RELAX_INTERVAL = 60 * 1000;

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLongRun() throws Exception {
        long start = System.currentTimeMillis();

        long lastPrint = start;

        do {
            clearCaches();

            testMultithreadedTaskRun();

            long now = System.currentTimeMillis();

            if (now - lastPrint > STATISTICS_PRINT_INTERVAL) {
                info(">>>>>>> Running test for " + ((now - start) / 1000) + " seconds.");

                lastPrint = now;
            }

            // Let idle check work.
            U.sleep(RELAX_INTERVAL);
        }
        while (System.currentTimeMillis() - start < TEST_RUN_TIME);
    }

    /** {@inheritDoc} */
    @Override protected int topologyRefreshFrequency() {
        return 5000;
    }

    /** {@inheritDoc} */
    @Override protected int maxConnectionIdleTime() {
        return topologyRefreshFrequency() / 5;
    }

    /**
     * Clears caches on all nodes.
     */
    private void clearCaches() {
        for (int i = 0; i < NODES_CNT; i++)
            try {
                grid(i).cache(PARTITIONED_CACHE_NAME).clear();
            } catch (IgniteException e) {
                log.error("Cache clear failed.", e);
            }
    }
}
