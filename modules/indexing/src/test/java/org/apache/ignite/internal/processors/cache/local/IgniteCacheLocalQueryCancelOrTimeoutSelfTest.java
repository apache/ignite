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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractQuerySelfTest;
import org.apache.ignite.internal.processors.query.h2.GridH2QueryCancelledException;

import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Tests local query cancellations and timeouts.
 */
public class IgniteCacheLocalQueryCancelOrTimeoutSelfTest extends IgniteCacheAbstractQuerySelfTest {
    /** Cache size. */
    private static int CACHE_SIZE = 10_000;

    /** */
    private static final String QUERY = "select a._val, b._val from String a, String b";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return LOCAL;
    }

    /**
     * @param cache Cache.
     */
    private void loadCache(IgniteCache<Integer, String> cache) {
        int p = 1;

        for (int i = 1; i <= CACHE_SIZE; i++) {
            char[] tmp = new char[256];
            Arrays.fill(tmp, ' ');
            cache.put(i, new String(tmp));

            if (i / (float)CACHE_SIZE >= p / 10f) {
                log().info("Loaded " + i + " of " + CACHE_SIZE);

                p++;
            }
        }
    }

    /**
     * Tests cancellation.
     */
    public void testQueryCancel() {
        testQuery(false, 1, TimeUnit.SECONDS);
    }

    /**
     * Tests timeout.
     */
    public void testQueryTimeout() {
        testQuery(true, 1, TimeUnit.SECONDS);
    }

    /**
     * Tests cancellation.
     */
    private void testQuery(boolean timeout, int timeoutUnits, TimeUnit timeUnit) {
        Ignite ignite = ignite();

        IgniteCache<Integer, String> cache = ignite.cache(null);

        loadCache(cache);

        SqlFieldsQuery qry = new SqlFieldsQuery(QUERY);

        final QueryCursor<List<?>> cursor;
        if (timeout) {
            qry.setTimeout(timeoutUnits, timeUnit);

            cursor = cache.query(qry);
        } else {
            cursor = cache.query(qry);

            ignite.scheduler().runLocal(new Runnable() {
                @Override public void run() {
                    cursor.close();
                }
            }, timeoutUnits, timeUnit);
        }

        try {
            cursor.iterator();

            fail("Expecting timeout");
        }
        catch (Exception e) {
            assertTrue("Must throw correct exception", e instanceof GridH2QueryCancelledException);
        }

        // Test must exit gracefully.
    }
}