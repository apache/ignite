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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Test;

/**
 *
 */
public class CacheGetRemoveSkipStoreTest extends GridCommonAbstractTest {
    /** */
    public static final String TEST_CACHE = "testCache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<String, String> ccfg = new CacheConfiguration<String, String>()
            .setCacheMode(CacheMode.PARTITIONED)
            .setName(TEST_CACHE)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(0)
            .setCacheStoreFactory(TestCacheStore::new)
            .setReadThrough(true)
            .setWriteThrough(false);

        cfg.setCacheConfiguration(ccfg);

        if (igniteInstanceName.contains("client"))
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);

        super.setUp();
    }

    /**
     */
    @After
    public void cleanUp() {
        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testNoNullReads() throws Exception {
        startGrid(0);

        IgniteEx client = startGrid("client");

        IgniteCache<Object, Object> cache = client.cache(TEST_CACHE);

        String key = "key";

        assertNotNull(cache.get(key));

        AtomicReference<String> failure = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean(false);

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            while (!stop.get()) {
                Object res = cache.get(key);

                if (res == null)
                    failure.compareAndSet(null, "Failed in thread: " + Thread.currentThread().getName());
            }
        });

        for (int i = 0; i < 100; i++)
            cache.remove(key);

        U.sleep(100);

        stop.set(true);
        fut.get();

        assertNotNull(cache.get(key));

        assertNull(failure.get());
    }

    /**
     * Dummy cache store which delays key load and loads a predefined value.
     */
    public static class TestCacheStore extends CacheStoreAdapter<String, String> {
        /** */
        static final String CONSTANT_VALUE = "expected_value";

        /** {@inheritDoc} */
        @Override public String load(String s) throws CacheLoaderException {
            try {
                U.sleep(1000);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new CacheLoaderException(e);
            }

            return CONSTANT_VALUE;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends String, ? extends String> entry) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object o) throws CacheWriterException {
            // No-op.
        }
    }
}
