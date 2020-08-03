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

package org.apache.ignite.internal.processors.query;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

/**
 *
 */
public class SqlIndexConsistencyAfterInterruptAtomicCacheOperationTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int KEYS = 1000;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     */
    protected CacheAtomicityMode atomicity() {
        return CacheAtomicityMode.ATOMIC;
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testPut() throws Exception {
        IgniteEx ign = startGrid(0);

        IgniteCache<Object, Object> cache = ign.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(atomicity())
            .setIndexedTypes(Integer.class, Integer.class));

        Thread t = new Thread(() -> {
            cache.put(1, 1);
        });

        t.start();

        t.interrupt();

        t.join();

        assertEquals(cache.size(), cache.query(new SqlFieldsQuery("select * from Integer")).getAll().size());
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testPutAll() throws Exception {
        IgniteEx ign = startGrid(0);

        IgniteCache<Object, Object> cache = ign.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(atomicity())
            .setIndexedTypes(Integer.class, Integer.class));

        final Map<Integer, Integer> batch = new HashMap<>();

        for (int i = 0; i < KEYS; ++i)
            batch.put(i, i);

        Thread t = new Thread(() -> {
            cache.putAll(batch);
        });

        t.start();
        t.interrupt();
        t.join();

        assertEquals(cache.size(), cache.query(new SqlFieldsQuery("select * from Integer")).getAll().size());
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testRemove() throws Exception {
        IgniteEx ign = startGrid(0);

        IgniteCache<Object, Object> cache = ign.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(atomicity())
            .setIndexedTypes(Integer.class, Integer.class));

        cache.put(1, 1);

        Thread t = new Thread(() -> {
            cache.remove(1);
        });

        t.start();

        t.interrupt();

        t.join();

        assertEquals(cache.size(), cache.query(new SqlFieldsQuery("select * from Integer")).getAll().size());
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testRemoveAll() throws Exception {
        IgniteEx ign = startGrid(0);

        IgniteCache<Object, Object> cache = ign.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(atomicity())
            .setIndexedTypes(Integer.class, Integer.class));

        final Map<Integer, Integer> batch = new HashMap<>();

        for (int i = 0; i < KEYS; ++i)
            batch.put(i, i);

        cache.putAll(batch);

        Thread t = new Thread(() -> {
            cache.removeAll(batch.keySet());
        });

        t.start();
        t.interrupt();
        t.join();

        assertEquals(cache.size(), cache.query(new SqlFieldsQuery("select * from Integer")).getAll().size());
    }
}
