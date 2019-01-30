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

package org.apache.ignite.internal.util.tostring;

import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.SensitiveInfoTestLoggerProxy;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.junit.Test;

/**
 * Tests for property {@link IgniteSystemProperties#IGNITE_TO_STRING_INCLUDE_SENSITIVE}.
 */
public abstract class IncludeSensitiveAbstractTest extends GridCacheAbstractSelfTest {
    /** Number of test entries */
    private static final int ENTRY_CNT = 10;

    /**
     * @param i Index.
     * @return Marked key by index.
     */
    private static Long key(int i) {
        return SensitiveInfoTestLoggerProxy.SENSITIVE_KEY_MARKER + i;
    }

    /**
     * @param i Index.
     * @return Marked value by index.
     */
    private static String value(int i) {
        return SensitiveInfoTestLoggerProxy.SENSITIVE_VAL_MARKER + i;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(IgniteConfiguration cfg, String cacheName) {
        CacheConfiguration ccfg = super.cacheConfiguration(cfg, cacheName);

        ccfg.setBackups(1);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE, "false");

        SensitiveInfoTestLoggerProxy.enableSensitiveMarkerAssertions(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        System.clearProperty(IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE);

        SensitiveInfoTestLoggerProxy.enableSensitiveMarkerAssertions(false);

        super.afterTestsStopped();
    }

    /** Start transaction. */
    protected void startTx() {
        // No-op.
    }

    /** Commit transaction. */
    protected void commitTx() {
        // No-op.
    }

    /**
     * Tests basic cache operations.
     *
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        IgniteCache<Long, String> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        startTx();

        for (int i = 1; i < ENTRY_CNT; ++i)
            cache.put(key(i), value(i));

        commitTx();

        cache.get(key(ENTRY_CNT / 2));

        for (int i = 1; i < ENTRY_CNT; ++i)
            cache.invoke(key(i), new TestEntryProcessor());

        stopGrid(1);

        cache.rebalance().get();

        for (int i = 0; i < ENTRY_CNT; ++i)
            cache.get(key(i));

        startGrid(1);

        cache.rebalance().get();

        startTx();

        for (int i = 1; i < ENTRY_CNT; ++i)
            cache.remove(key(i));

        commitTx();
    }

    /**
     *
     */
    static class TestEntryProcessor implements CacheEntryProcessor<Long, String, Object> {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Long, String> entry, Object... args) {
            long key = entry.getKey();

            if (key % 2 == 0)
                entry.remove();
            else
                entry.setValue(entry.getValue() + "-");

            return null;
        }
    }
}
