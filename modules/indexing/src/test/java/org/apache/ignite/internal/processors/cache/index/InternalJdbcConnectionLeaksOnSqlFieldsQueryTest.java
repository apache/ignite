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

package org.apache.ignite.internal.processors.cache.index;

import java.sql.Connection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for leaks JdbcConnection on SqlFieldsQuery execute.
 */
public class InternalJdbcConnectionLeaksOnSqlFieldsQueryTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Threads count. */
    private static final int THREADS_COUNT = 10000;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final CacheConfiguration ccfg = new CacheConfiguration().setName(CACHE_NAME);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(ccfg);

        if (getTestIgniteInstanceIndex(igniteInstanceName) != 0)
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     * @throws Exception On failed.
     */
    public void testConnectionLeaks() throws Exception {
        final IgniteCache cache = grid(1).cache(CACHE_NAME);

        final CountDownLatch c = new CountDownLatch(THREADS_COUNT);

        for (int i = 0; i < THREADS_COUNT; i++) {
            new Thread() {
                @Override public void run() {
                    SqlFieldsQuery qry = new SqlFieldsQuery("select 1").setLocal(false);

                    final QueryCursor cur = cache.query(qry);

                    cur.getAll().size();

                    cur.close();

                    c.countDown();
                }
            }.start();
        }

        c.await();

        final Map<Thread, Connection> conns = GridTestUtils.getFieldValue(grid(1).context().query().getIndexing(), "conns");

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return conns.size() == 1;
            }
        }, 5000);

        assertEquals("Invalid opened connection count", 1, conns.size());
    }
}
