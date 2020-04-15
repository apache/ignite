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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;

/**
 * Abstract class to test that a remote filter and transformer of ContinuousQueries run on a remote node inside the
 * Ignite Sandbox.
 */
public class AbstractContinuousQuerySandboxTest extends AbstractSandboxTest {
    /** Cache name for tests. */
    protected static final String CACHE_NAME = "TEST_CACHE";

    /** Cache index. */
    private static final AtomicInteger CACHE_INDEX = new AtomicInteger();

    /** Error message. */
    private static final String ERROR_MESSAGE = "java.security.AccessControlException: " +
        "access denied (\"java.util.PropertyPermission\" \"test.sandbox.property\" \"write\")";

    /** Log listener. */
    private static final LogListener LOG_LSNR = LogListener
        .matches(s -> s.contains(ERROR_MESSAGE))
        .times(1)
        .build();

    /** Test logger. */
    private static ListeningTestLogger log;

    /** Preidacte for inital query tests. */
    protected static IgniteBiPredicate<Integer, Integer> initQryFilter;

    /** Remote filter. */
    protected static CacheEntryEventSerializableFilter<Integer, Integer> rmtFilter;

    /** Constructor. */
    public AbstractContinuousQuerySandboxTest() {
        log = new ListeningTestLogger(false, GridAbstractTest.log);

        log.registerListener(LOG_LSNR);

        initQryFilter = (k, v) -> {
            try {
                controlAction();
            }
            catch (Throwable e) {
                log.error("", e);
            }

            return true;
        };

        rmtFilter = e -> {
            controlAction();

            return true;
        };
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setGridLogger(log);
    }

    /** */
    protected void checkContinuousQuery(Supplier<Query<Cache.Entry<Integer, Integer>>> s, boolean init) {
        runOperation(() -> executeQuery(grid(CLNT_ALLOWED_WRITE_PROP), s.get(), init));
        runForbiddenOperation(() -> executeQuery(grid(CLNT_FORBIDDEN_WRITE_PROP), s.get(), init));
    }

    /**
     * @param r RunnableX that that runs {@link AbstractSandboxTest#controlAction()}.
     */
    private void runForbiddenOperation(GridTestUtils.RunnableX r) {
        LOG_LSNR.reset();
        System.clearProperty(PROP_NAME);

        r.run();

        assertTrue(LOG_LSNR.check());
        assertNull(System.getProperty(PROP_NAME));
    }

    /**
     * Opens query cursor.
     *
     * @param q {@link Query}.
     * @param init True if needing put data to a cache before openning a cursor.
     */
    private void executeQuery(Ignite ignite, Query<Cache.Entry<Integer, Integer>> q, boolean init) {
        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>()
                .setName(CACHE_NAME + CACHE_INDEX.incrementAndGet())
                .setCacheMode(CacheMode.PARTITIONED)
        );

        if (init)
            cache.put(primaryKey(grid(SRV), cache.getName()), 100);

        try (QueryCursor<Cache.Entry<Integer, Integer>> cur = cache.query(q)) {
            if (!init)
                cache.put(primaryKey(grid(SRV), cache.getName()), 100);

            cur.getAll();
        }
    }

    /**
     * Getting the key that is contained on primary partition on passed node for cache.
     *
     * @param ignite Node.
     * @param cacheName Cache name.
     * @return Key.
     */
    private Integer primaryKey(IgniteEx ignite, String cacheName) {
        return findKeys(ignite.localNode(), ignite.cache(cacheName), 1, 0, 0)
            .stream()
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(ignite.name() + " isn't primary node for any key."));
    }
}
