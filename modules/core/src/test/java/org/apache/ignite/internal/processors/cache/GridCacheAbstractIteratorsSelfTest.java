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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.testframework.*;

import java.util.*;

/**
 * Tests for cache iterators.
 */
public abstract class GridCacheAbstractIteratorsSelfTest extends GridCacheAbstractSelfTest {
    /** Key prefix. */
    protected static final String KEY_PREFIX = "testKey";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        for (int i = 0; i < entryCount(); i++)
            cache().put(KEY_PREFIX + i, i);
    }

    /**
     * @return Entry count.
     */
    protected abstract int entryCount();

    /**
     * @throws Exception If failed.
     */
    public void testCacheIterator() throws Exception {
        int cnt = 0;

        for (CacheEntry<String, Integer> entry : cache()) {
            assert entry != null;
            assert entry.getKey() != null;
            assert entry.getKey().contains(KEY_PREFIX);
            assert entry.getValue() != null;
            assert entry.getValue() >= 0 && entry.getValue() < entryCount();
            assert entry.get() != null;
            assert entry.get() >= 0 && entry.get() < entryCount();

            cnt++;
        }

        assertEquals(cnt, entryCount());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheProjectionIterator() throws Exception {
        int cnt = 0;

        for (CacheEntry<String, Integer> entry : cache().projection(lt50)) {
            assert entry != null;
            assert entry.getKey() != null;
            assert entry.getKey().contains(KEY_PREFIX);
            assert entry.getValue() != null;
            assert entry.getValue() >= 0 && entry.getValue() < 50;
            assert entry.get() != null;
            assert entry.get() >= 0 && entry.get() < 50;

            cnt++;
        }

        assert cnt == 50;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheIteratorMultithreaded() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            cache(i).removeAll();

        final IgniteFuture<?> putFut = GridTestUtils.runMultiThreadedAsync(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                for (int i = 0; i < entryCount(); i++)
                    cache().put(KEY_PREFIX + i, i);
            }
        }, 1, "put-thread");

        GridTestUtils.runMultiThreaded(new CA() {
            @Override public void apply() {
                while (!putFut.isDone()) {
                    for (CacheEntry<String, Integer> entry : cache()) {
                        assert entry != null;
                        assert entry.getKey() != null;
                        assert entry.getKey().contains(KEY_PREFIX);
                    }
                }
            }
        }, 3, "iterator-thread");
    }

    /**
     * @throws Exception If failed.
     */
    public void testEntrySetIterator() throws Exception {
        Set<CacheEntry<String, Integer>> entries = cache().entrySet();

        assert entries != null;
        assert entries.size() == entryCount();

        int cnt = 0;

        for (CacheEntry<String, Integer> entry : entries) {
            assert entry != null;
            assert entry.getKey() != null;
            assert entry.getKey().contains(KEY_PREFIX);
            assert entry.getValue() != null;
            assert entry.getValue() >= 0 && entry.getValue() < entryCount();
            assert entry.get() != null;
            assert entry.get() >= 0 && entry.get() < entryCount();

            cnt++;
        }

        assert cnt == entryCount();
    }

    /**
     * @throws Exception If failed.
     */
    public void testEntrySetIteratorFiltered() throws Exception {
        Set<CacheEntry<String, Integer>> entries = cache().projection(lt50).entrySet();

        assert entries != null;
        assert entries.size() == 50;

        int cnt = 0;

        for (CacheEntry<String, Integer> entry : entries) {
            assert entry != null;
            assert entry.getKey() != null;
            assert entry.getKey().contains(KEY_PREFIX);
            assert entry.getValue() != null;
            assert entry.getValue() >= 0 && entry.getValue() < 50;
            assert entry.get() != null;
            assert entry.get() >= 0 && entry.get() < 50;

            cnt++;
        }

        assert cnt == 50;
    }

    /**
     * @throws Exception If failed.
     */
    public void testEntrySetIteratorMultithreaded() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            cache(i).removeAll();

        final IgniteFuture<?> putFut = GridTestUtils.runMultiThreadedAsync(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                for (int i = 0; i < entryCount(); i++)
                    cache().put(KEY_PREFIX + i, i);
            }
        }, 1, "put-thread");

        GridTestUtils.runMultiThreaded(new CA() {
            @Override public void apply() {
                while (!putFut.isDone()) {
                    for (CacheEntry<String, Integer> entry : cache().entrySet()) {
                        assert entry != null;
                        assert entry.getKey() != null;
                        assert entry.getKey().contains(KEY_PREFIX);
                    }
                }
            }
        }, 3, "iterator-thread");
    }

    /**
     * @throws Exception If failed.
     */
    public void testKeySetIterator() throws Exception {
        Set<String> keys = cache().keySet();

        assert keys != null;
        assert keys.size() == entryCount();

        List<Integer> values = new ArrayList<>(entryCount());

        int cnt = 0;

        for (String key : keys) {
            assert key != null;
            assert key.contains(KEY_PREFIX);

            values.add(cache().get(key));

            cnt++;
        }

        assert values.size() == entryCount();
        assert cnt == entryCount();

        Collections.sort(values);

        for (int i = 0; i < values.size(); i++)
            assert values.get(i) == i;
    }

    /**
     * @throws Exception If failed.
     */
    public void testKeySetIteratorFiltered() throws Exception {
        Set<String> keys = cache().projection(lt50).keySet();

        assert keys != null;
        assert keys.size() == 50;

        List<Integer> values = new ArrayList<>(50);

        int cnt = 0;

        for (String key : keys) {
            assert key != null;
            assert key.contains(KEY_PREFIX);

            values.add(cache().get(key));

            cnt++;
        }

        assert values.size() == 50;
        assert cnt == 50;

        Collections.sort(values);

        for (int i = 0; i < values.size(); i++)
            assert values.get(i) == i;
    }

    /**
     * @throws Exception If failed.
     */
    public void testKeySetIteratorMultithreaded() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            cache(i).removeAll();

        final IgniteFuture<?> putFut = GridTestUtils.runMultiThreadedAsync(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                for (int i = 0; i < entryCount(); i++)
                    cache().put(KEY_PREFIX + i, i);
            }
        }, 1, "put-thread");

        GridTestUtils.runMultiThreaded(new CA() {
            @Override public void apply() {
                while (!putFut.isDone()) {
                    for (String key : cache().keySet()) {
                        assert key != null;
                        assert key.contains(KEY_PREFIX);
                    }
                }
            }
        }, 3, "iterator-thread");
    }

    /**
     * @throws Exception If failed.
     */
    public void testValuesIterator() throws Exception {
        Collection<Integer> values = cache().values();

        assert values != null;
        assert values.size() == entryCount();

        int cnt = 0;

        for (Integer value : values) {
            assert value != null;
            assert value >= 0 && value < entryCount();

            cnt++;
        }

        assert cnt == entryCount();
    }

    /**
     * @throws Exception If failed.
     */
    public void testValuesIteratorFiltered() throws Exception {
        Collection<Integer> values = cache().projection(lt50).values();

        assert values != null;
        assert values.size() == 50;

        int cnt = 0;

        for (Integer value : values) {
            assert value != null;
            assert value >= 0 && value < 50;

            cnt++;
        }

        assert cnt == 50;
    }

    /**
     * @throws Exception If failed.
     */
    public void testValuesIteratorMultithreaded() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            cache(i).removeAll();

        final IgniteFuture<?> putFut = GridTestUtils.runMultiThreadedAsync(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                for (int i = 0; i < entryCount(); i++)
                    cache().put(KEY_PREFIX + i, i);
            }
        }, 1, "put-thread");

        GridTestUtils.runMultiThreaded(new CA() {
            @Override public void apply() {
                while (!putFut.isDone()) {
                    for (Integer value : cache().values())
                        assert value != null;
                }
            }
        }, 3, "iterator-thread");
    }
}
