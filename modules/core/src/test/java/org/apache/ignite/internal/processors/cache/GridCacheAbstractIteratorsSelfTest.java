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
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.CA;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.testframework.GridTestUtils;

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
            jcache().put(KEY_PREFIX + i, i);
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

        for (Cache.Entry<String, Integer> entry : jcache()) {
            assert entry != null;
            assert entry.getKey() != null;
            assert entry.getKey().contains(KEY_PREFIX);
            assert entry.getValue() != null;
            assert entry.getValue() >= 0 && entry.getValue() < entryCount();
            assert entry.getValue() != null;
            assert entry.getValue() >= 0 && entry.getValue() < entryCount();

            cnt++;
        }

        assertEquals(cnt, entryCount());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheIteratorMultithreaded() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            jcache(i).removeAll();

        final IgniteInternalFuture<?> putFut = GridTestUtils.runMultiThreadedAsync(new CAX() {
            @Override public void applyx() {
                for (int i = 0; i < entryCount(); i++)
                    jcache().put(KEY_PREFIX + i, i);
            }
        }, 1, "put-thread");

        GridTestUtils.runMultiThreaded(new CA() {
            @Override public void apply() {
                while (!putFut.isDone()) {
                    for (Cache.Entry<String, Integer> entry : jcache()) {
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
        assert jcache().localSize(CachePeekMode.ALL) == entryCount();

        int cnt = 0;

        for (Cache.Entry<String, Integer> entry : jcache()) {
            assert entry != null;
            assert entry.getKey() != null;
            assert entry.getKey().contains(KEY_PREFIX);
            assert entry.getValue() != null;
            assert entry.getValue() >= 0 && entry.getValue() < entryCount();
            assert entry.getValue() != null;
            assert entry.getValue() >= 0 && entry.getValue() < entryCount();

            cnt++;
        }

        assert cnt == entryCount();
    }

    /**
     * @throws Exception If failed.
     */
    public void testEntrySetIteratorMultithreaded() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            jcache(i).removeAll();

        final IgniteInternalFuture<?> putFut = GridTestUtils.runMultiThreadedAsync(new CAX() {
            @Override public void applyx() {
                for (int i = 0; i < entryCount(); i++)
                    jcache().put(KEY_PREFIX + i, i);
            }
        }, 1, "put-thread");

        GridTestUtils.runMultiThreaded(new CA() {
            @Override public void apply() {
                while (!putFut.isDone()) {
                    for (Cache.Entry<String, Integer> entry : jcache()) {
                        assert entry != null;
                        assert entry.getKey() != null;
                        assert entry.getKey().contains(KEY_PREFIX);
                    }
                }
            }
        }, 3, "iterator-thread");
    }

}