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

package org.apache.ignite.internal.processors.cache.local;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.timeout.TimedQueryHelper;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Tests local query cancellations and timeouts.
 */
public class IgniteCacheLocalQueryCancelOrTimeoutSelfTest extends GridCommonAbstractTest {
    /** Cache size. */
    private static final int CACHE_SIZE = 1_000;

    /** */
    private static final String QUERY = "select a._val, b._val, longProcess(a._key, 5) from String a, String b";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<Integer, String>(DEFAULT_CACHE_NAME)
            .setIndexedTypes(Integer.class, String.class)
            .setCacheMode(LOCAL)
            .setSqlFunctionClasses(TimedQueryHelper.class);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);

        Ignite ignite = grid(0);

        IgniteCache<Integer, String> cache = ignite.cache(DEFAULT_CACHE_NAME);

        loadCache(cache);
    }

    /**
     * @param cache Cache.
     */
    private void loadCache(IgniteCache<Integer, String> cache) {
        int p = 1;

        Map<Integer, String> batch = new HashMap<>();

        for (int i = 1; i <= CACHE_SIZE; i++) {
            char[] tmp = new char[256];
            Arrays.fill(tmp, ' ');
            batch.put(i, new String(tmp));

            if (i / (float)CACHE_SIZE >= p / 10f) {
                cache.putAll(batch);

                batch.clear();

                log().info("Loaded " + i + " of " + CACHE_SIZE);

                p++;
            }
        }

        if (!F.isEmpty(batch))
            cache.putAll(batch);
    }

    /**
     * Tests cancellation.
     */
    @Test
    public void testQueryCancel() {
        testQuery(false, 1, TimeUnit.SECONDS);
    }

    /**
     * Tests cancellation with zero timeout.
     */
    @Test
    public void testQueryCancelZeroTimeout() {
        testQuery(false, 1, TimeUnit.MILLISECONDS);
    }

    /**
     * Tests timeout.
     */
    @Test
    public void testQueryTimeout() {
        testQuery(true, 1, TimeUnit.SECONDS);
    }

    /**
     * Tests cancel multithreaded.
     */
    @Test
    public void testQueryCancelMultithreaded() throws Exception {
        GridTestUtils.runMultiThreaded(() -> {
            for (int i = 0; i < 20; ++i)
                testQuery(false, 500, TimeUnit.MILLISECONDS);

            return null;
        }, 20, "local-cancel-test");
    }

    /**
     * Tests cancellation.
     */
    private void testQuery(boolean timeout, int timeoutUnits, TimeUnit timeUnit) {
        Ignite ignite = grid(0);

        IgniteCache<Integer, String> cache = ignite.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery(QUERY);

        final QueryCursor<List<?>> cursor;
        if (timeout) {
            qry.setTimeout(timeoutUnits, timeUnit);

            cursor = cache.query(qry);
        }
        else {
            cursor = cache.query(qry);

            ignite.scheduler().runLocal(new Runnable() {
                @Override public void run() {
                    cursor.close();
                }
            }, timeoutUnits, timeUnit);
        }

        try (QueryCursor<List<?>> ignored = cursor) {
            Iterator<List<?>> it = cursor.iterator();

            int cnt = 0;

            while (it.hasNext()) {
                it.next();

                ++cnt;
            }

            fail("Expecting timeout or cancel. Results size=" + cnt);
        }
        catch (Throwable e) {
            if (X.cause(e, QueryCancelledException.class) == null) {
                log.error("Unexpected exception", e);

                fail("Must throw correct exception");
            }
        }
    }
}
