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

package org.apache.ignite.internal.processors.security.sandbox;

import java.security.AccessControlException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Abstract class to test that a remote filter and transformer of ContinuousQueries run on a remote node inside the
 * Ignite Sandbox.
 */
public class AbstractContinuousQuerySandboxTest extends AbstractSandboxTest {
    /** Cache name for tests. */
    protected static final String CACHE_NAME = "TEST_CACHE";

    /** Cache index. */
    private static final AtomicInteger CACHE_INDEX = new AtomicInteger();

    /** Error. */
    private static volatile AccessControlException error;

    /** Runs control action for CQ tests. */
    protected static final Runnable CONTROL_ACTION_RUNNER = new Runnable() {
        @Override public void run() {
            try {
                controlAction();
            }
            catch (AccessControlException e) {
                error = e;

                throw e;
            }
        }
    };

    /** Preidacte for inital query tests. */
    protected static final IgniteBiPredicate<Integer, Integer> INIT_QRY_FILTER = (k, v) -> {
        CONTROL_ACTION_RUNNER.run();

        return true;
    };

    /** Remote filter. */
    protected static final CacheEntryEventSerializableFilter<Integer, Integer> RMT_FILTER = ent -> {
        CONTROL_ACTION_RUNNER.run();

        return true;
    };

    /** */
    protected void checkContinuousQuery(Supplier<Query<Cache.Entry<Integer, Integer>>> s, boolean init) {
        runOperation(operation(CLNT_ALLOWED_WRITE_PROP, s, init));
        runForbiddenOperation(operation(CLNT_FORBIDDEN_WRITE_PROP, s, init), AccessControlException.class);
    }

    /** */
    private GridTestUtils.RunnableX operation(String nodeName,
        Supplier<Query<Cache.Entry<Integer, Integer>>> s, boolean init) {
        return () -> {
            error = null;

            executeQuery(grid(nodeName), s.get(), init);

            if (error != null)
                throw error;
        };
    }

    /**
     * Opens query cursor.
     *
     * @param ignite Node.
     * @param q {@link Query}.
     * @param init True if needing put data to a cache before openning a cursor.
     */
    private void executeQuery(Ignite ignite, Query<Cache.Entry<Integer, Integer>> q, boolean init)
        throws IgniteCheckedException {
        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>()
                .setName(CACHE_NAME + CACHE_INDEX.incrementAndGet())
                .setCacheMode(CacheMode.PARTITIONED)
        );

        Integer key = primaryKey(grid(SRV).cache(cache.getName()));

        if (init)
            cache.put(key, 100);

        try (QueryCursor<Cache.Entry<Integer, Integer>> cur = cache.query(q)) {
            if (!init) {
                try {
                    cache.put(key, 100);
                }
                catch (Exception e) {
                    fail(e.getMessage());
                }
            }

            cur.getAll();
        }

        // Put operation should be successful regardless of exceptions inside a remote filter or transformer.
        assertEquals(Integer.valueOf(100), cache.get(key));
    }
}
