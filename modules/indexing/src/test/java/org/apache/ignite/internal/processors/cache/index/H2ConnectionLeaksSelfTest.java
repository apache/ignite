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

package org.apache.ignite.internal.processors.cache.index;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test for leaks JdbcConnection on SqlFieldsQuery execute.
 */
@RunWith(JUnit4.class)
public class H2ConnectionLeaksSelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Nodes count. */
    private static final int NODE_CNT = 2;

    /** Keys count. */
    private static final int KEY_CNT = 100;

    /** Threads count. */
    private static final int THREAD_CNT = 100;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite node = startGrids(NODE_CNT);

        IgniteCache<Long, String> cache = node.cache(CACHE_NAME);

        for (int i = 0; i < KEY_CNT; i++)
            cache.put((long)i, String.valueOf(i));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration<Long, String> ccfg = new CacheConfiguration<Long, String>().setName(CACHE_NAME)
            .setIndexedTypes(Long.class, String.class);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(ccfg);

        if (getTestIgniteInstanceIndex(igniteInstanceName) != 0)
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     * @throws Exception On failed.
     */
    @Test
    public void testConnectionLeaks() throws Exception {
        final IgniteCache cache = grid(1).cache(CACHE_NAME);

        final CountDownLatch latch = new CountDownLatch(THREAD_CNT);

        for (int i = 0; i < THREAD_CNT; i++) {
            new Thread() {
                @Override public void run() {
                    SqlFieldsQuery qry = new SqlFieldsQuery("select * from String").setLocal(false);

                    cache.query(qry).getAll();

                    latch.countDown();
                }
            }.start();
        }

        latch.await();

        checkConnectionLeaks();
    }

    /**
     * @throws Exception On failed.
     */
    @Test
    public void testConnectionLeaksOnSqlException() throws Exception {
        final CountDownLatch latch = new CountDownLatch(THREAD_CNT);
        final CountDownLatch latch2 = new CountDownLatch(1);

        for (int i = 0; i < THREAD_CNT; i++) {
            new Thread() {
                @Override public void run() {
                    try {
                        IgniteH2Indexing idx = (IgniteH2Indexing)grid(1).context().query().getIndexing();

                        idx.executeStatement(CACHE_NAME, "select *");
                    }
                    catch (Exception e) {
                        // No-op.
                    }

                    latch.countDown();

                    try {
                        latch2.await();
                    }
                    catch (InterruptedException e) {
                        // No-op;
                    }
                }
            }.start();
        }

        try {
            latch.await();

            checkConnectionLeaks();
        }
        finally {
            latch2.countDown();
        }
    }

    /**
     * @throws Exception On error.
     */
    private void checkConnectionLeaks() throws Exception {
        boolean notLeak = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (int i = 0; i < NODE_CNT; i++) {
                    Map<Thread, ?> conns = perThreadConnections(i);

                    for(Thread t : conns.keySet()) {
                        if (!t.isAlive())
                            return false;
                    }
                }

                return true;
            }
        }, 5000);

        if (!notLeak) {
            for (int i = 0; i < NODE_CNT; i++) {
                Map<Thread, ?> conns = perThreadConnections(i);

                for(Thread t : conns.keySet())
                    log.error("+++ Connection is not closed for thread: " + t.getName());
            }

            fail("H2 JDBC connections leak detected. See the log above.");
        }
    }

    /**
     * @param nodeIdx Node index.
     * @return Per-thread connections.
     */
    private Map<Thread, ?> perThreadConnections(int nodeIdx) {
        return ((IgniteH2Indexing)grid(nodeIdx).context().query().getIndexing()).perThreadConnections();
    }
}
