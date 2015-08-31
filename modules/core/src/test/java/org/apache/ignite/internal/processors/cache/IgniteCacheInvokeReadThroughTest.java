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

import javax.cache.configuration.Factory;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.NearCacheConfiguration;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class IgniteCacheInvokeReadThroughTest extends IgniteCacheAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-114");
    }

    /** */
    private static volatile boolean failed;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected Factory<CacheStore> cacheStoreFactory() {
        return new TestStoreFactory();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        failed = false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThrough() throws Exception {
        IgniteCache<Integer, Integer> cache = jcache(0);

        checkReadThrough(cache, primaryKey(cache));

        checkReadThrough(cache, backupKey(cache));

        checkReadThrough(cache, nearKey(cache));
    }

    /**
     * @param cache Cache.
     * @param key Key.
     */
    private void checkReadThrough(IgniteCache<Integer, Integer> cache, Integer key) {
        log.info("Test [key=" + key + ']');

        storeMap.put(key, key);

        Object ret = cache.invoke(key, new EntryProcessor<Integer, Integer, Object>() {
            @Override public Object process(MutableEntry<Integer, Integer> entry, Object... args) {
                if (!entry.exists()) {
                    failed = true;

                    fail();
                }

                Integer val = entry.getValue();

                if (!val.equals(entry.getKey())) {
                    failed = true;

                    assertEquals(val, entry.getKey());
                }

                entry.setValue(val + 1);

                return val;
            }
        });

        assertEquals(key, ret);

        for (int i = 0; i < gridCount(); i++)
            assertEquals("Unexpected value for node: " + i, key + 1, jcache(i).get(key));

        assertFalse(failed);
    }
}
