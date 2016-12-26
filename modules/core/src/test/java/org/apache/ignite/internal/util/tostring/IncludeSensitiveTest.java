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

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridLoggerProxy;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.apache.ignite.transactions.Transaction;

/**
 * Tests for property {@link IgniteSystemProperties#IGNITE_TO_STRING_INCLUDE_SENSITIVE}.
 */
@GridCommonTest(group = "Utils")
public abstract class IncludeSensitiveTest extends GridCacheAbstractSelfTest {

    /** Number of test entries */
    private static final int ENTRY_CNT = 10;

    /** Get marked key by index */
    private static Long key(int i) {
        return GridLoggerProxy.SENSITIVE_KEY_MARKER + i;
    }

    /** Get marked value by index */
    private static String value(int i) {
        return GridLoggerProxy.SENSITIVE_VAL_MARKER + i;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    @Override protected CacheConfiguration cacheConfiguration(IgniteConfiguration cfg, String cacheName) {
        CacheConfiguration ccfg = super.cacheConfiguration(cfg, cacheName);
        ccfg.setBackups(1);
        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();
        GridLoggerProxy.enableSensitiveMarkerAssertions(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        GridLoggerProxy.enableSensitiveMarkerAssertions(false);
        super.afterTestsStopped();
    }

    /** Start transaction */
    protected void startTx() {
    }

    /** Commit transaction */
    protected void commitTx() {
    }

    /** Testing basic cache operations */
    public void test() throws Exception {
        IgniteCache<Long, String> cache = grid(0).cache(null);
        startTx();
        for (int i = 1; i < ENTRY_CNT; ++i)
            cache.put(key(i), value(i));
        commitTx();
        cache.get(key(ENTRY_CNT / 2));
        for (int i = 1; i < ENTRY_CNT; ++i)
            cache.invoke(key(i), new CacheEntryProcessor<Long, String, Object>() {
                @Override
                public Object process(MutableEntry<Long, String> entry, Object... arguments)
                    throws EntryProcessorException {

                    long key = entry.getKey();
                    if (key % 2 == 0)
                        entry.remove();
                    else
                        entry.setValue(entry.getValue() + "-");
                    return null;
                }
            });
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
}