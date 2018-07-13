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

package org.apache.ignite.internal.processors.cache;

import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * ScanQuery test.
 */
public class CacheScanQueryTest extends GridCommonAbstractTest {
    /** Client mode. */
    private boolean client = false;

    /** Cache configurations. */
    private CacheConfiguration[] ccfgs = null;

    /** */
    private GridStringLogger strLogger;

    /** */
    public CacheScanQueryTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        client = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setClientMode(client);
        cfg.setCacheConfiguration(ccfgs);

        if (strLogger != null)
            cfg.setGridLogger(strLogger);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQuery() throws Exception {
        strLogger = new GridStringLogger();

        Ignite ignite = startGrid(0);
        startGrid(1);

        ignite.cluster().active(true);

        client = true;

        ccfgs = new CacheConfiguration[] {
            new CacheConfiguration("test-cache-replicated-atomic")
                .setCacheMode(REPLICATED)
                .setAtomicityMode(ATOMIC),
            new CacheConfiguration("test-cache-replicated-transactional")
                .setCacheMode(REPLICATED)
                .setAtomicityMode(TRANSACTIONAL),
            new CacheConfiguration("test-cache-partitioned-atomic")
                .setCacheMode(PARTITIONED)
                .setAtomicityMode(ATOMIC),
            new CacheConfiguration("test-cache-partitioned-transactional")
                .setCacheMode(PARTITIONED)
                .setAtomicityMode(TRANSACTIONAL)
        };

        Ignite client = startGrid(2);

        IgniteBinary binary = client.binary();

        final int n = 1_000;

        for (CacheConfiguration cfg : ccfgs) {
            IgniteCache<Object, Object> cache = client.cache(cfg.getName()).withKeepBinary();

            for (int i = 0; i < n; i++)
                cache.put(i, binary.builder("type_name").setField("f_" + i, "v_" + i).build());
        }

        for (CacheConfiguration cfg : ccfgs) {
            IgniteCache<Object, Object> cache = client.cache(cfg.getName()).withKeepBinary();

            int actualN = 0;

            try (QueryCursor<Cache.Entry<Integer, BinaryObject>> cursor = cache.query(new ScanQuery<>())) {
                for (Cache.Entry<Integer, BinaryObject> entry : cursor)
                    actualN++;
            }

            assertEquals(n, actualN);
        }

        final IgniteBiPredicate<Integer, BinaryObject> predicate = new IgniteBiPredicate<Integer, BinaryObject>() {
            @Override public boolean apply(Integer key, BinaryObject value) {
                return value.field(null) != null;
            }
        };

        for (CacheConfiguration cfg : ccfgs) {
            IgniteCache<Object, Object> cache = client.cache(cfg.getName()).withKeepBinary();

            int actualN = 0;

            try (QueryCursor<Cache.Entry<Integer, BinaryObject>> cursor = cache.withKeepBinary().query(new ScanQuery<>(
                predicate)))
            {
                for (Cache.Entry<Integer, BinaryObject> entry : cursor)
                    actualN++;

                fail();
            }
            catch (Throwable e) {
                assertTrue(e instanceof CacheException);
            }

            assertEquals(0, actualN);
        }

        assertTrue(!strLogger.toString().contains("Critical system error detected."));
    }
}