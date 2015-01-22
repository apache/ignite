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

package org.apache.ignite.internal.processors.cache.integration;

import org.apache.ignite.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.lifecycle.*;
import org.apache.ignite.resources.*;
import org.jdk8.backport.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.integration.*;
import javax.cache.processor.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public abstract class IgniteCacheLoaderWriterAbstractTest extends IgniteCacheAbstractTest {
    /** */
    private static AtomicInteger ldrCallCnt = new AtomicInteger();

    /** */
    private static AtomicInteger writerCallCnt = new AtomicInteger();

    /** */
    private static ConcurrentHashMap8<Object, Object> storeMap = new ConcurrentHashMap8<>();

    /** {@inheritDoc} */
    @Override protected Factory<? extends CacheLoader> loaderFactory() {
        return new Factory<CacheLoader>() {
            @Override public CacheLoader create() {
                return new TestLoader();
            }
        };
    }

    /** {@inheritDoc} */
    @Override protected Factory<? extends CacheWriter> writerFactory() {
        return new Factory<CacheWriter>() {
            @Override public CacheWriter create() {
                return new TestWriter();
            }
        };
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        ldrCallCnt.set(0);

        writerCallCnt.set(0);

        storeMap.clear();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoaderWriter() throws Exception {
        IgniteCache<Object, Object> cache = jcache(0);

        Object key = primaryKey(cache);

        assertNull(cache.get(key));

        checkCalls(1, 0);

        storeMap.put(key, "test");

        assertEquals("test", cache.get(key));

        checkCalls(1, 0);

        assertTrue(storeMap.containsKey(key));

        cache.remove(key);

        checkCalls(0, 1);

        assertFalse(storeMap.containsKey(key));

        assertNull(cache.get(key));

        checkCalls(1, 0);

        cache.put(key, "test1");

        checkCalls(0, 1);

        assertEquals("test1", storeMap.get(key));

        assertEquals("test1", cache.get(key));

        checkCalls(0, 0);

        cache.invoke(key, new EntryProcessor<Object, Object, Object>() {
            @Override public Object process(MutableEntry<Object, Object> e, Object... args) {
                e.setValue("test2");

                return null;
            }
        });

        checkCalls(0, 1);

        assertEquals("test2", storeMap.get(key));

        assertEquals("test2", cache.get(key));

        checkCalls(0, 0);

        cache.invoke(key, new EntryProcessor<Object, Object, Object>() {
            @Override public Object process(MutableEntry<Object, Object> e, Object... args) {
                e.remove();

                return null;
            }
        });

        checkCalls(0, 1);

        assertFalse(storeMap.containsKey(key));

        assertNull(cache.get(key));
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoaderWriterBulk() throws Exception {
        Map<Object, Object> vals = new HashMap<>();

        IgniteCache<Object, Object> cache = jcache(0);

        for (Object key : primaryKeys(cache, 100, 0))
            vals.put(key, key);

        assertTrue(cache.getAll(vals.keySet()).isEmpty());

        checkCalls(1, 0);

        storeMap.putAll(vals);

        assertEquals(vals, cache.getAll(vals.keySet()));

        checkCalls(1, 0);

        for (Object key : vals.keySet())
            assertEquals(key, storeMap.get(key));

        cache.removeAll(vals.keySet());

        checkCalls(0, 1);

        for (Object key : vals.keySet())
            assertFalse(storeMap.containsKey(key));

        cache.putAll(vals);

        checkCalls(0, 1);

        for (Object key : vals.keySet())
            assertEquals(key, storeMap.get(key));

        cache.invokeAll(vals.keySet(), new EntryProcessor<Object, Object, Object>() {
            @Override public Object process(MutableEntry<Object, Object> entry, Object... args) {
                entry.setValue("test1");

                return null;
            }
        });

        checkCalls(0, 1);

        for (Object key : vals.keySet())
            assertEquals("test1", storeMap.get(key));

        cache.invokeAll(vals.keySet(), new EntryProcessor<Object, Object, Object>() {
            @Override public Object process(MutableEntry<Object, Object> entry, Object... args) {
                entry.remove();

                return null;
            }
        });

        checkCalls(0, 1);

        for (Object key : vals.keySet())
            assertFalse(storeMap.containsKey(key));
    }

    /**
     * @param expLdr Expected loader calls count.
     * @param expWriter Expected writer calls count.
     */
    private void checkCalls(int expLdr, int expWriter) {
        assertEquals(expLdr, ldrCallCnt.get());
        assertEquals(expWriter, writerCallCnt.get());

        ldrCallCnt.set(0);
        writerCallCnt.set(0);
    }

    /**
     *
     */
    class TestLoader implements CacheLoader<Object, Object>, LifecycleAware {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private boolean startCalled;

        /** {@inheritDoc} */
        @Override public void start() throws IgniteCheckedException {
            startCalled = true;

            assertNotNull(ignite);
        }

        /** {@inheritDoc} */
        @Override public void stop() throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) {
            assertTrue(startCalled);

            assertNotNull(ignite);

            log.info("Load: " + key);

            ldrCallCnt.incrementAndGet();

            return storeMap.get(key);
        }

        /** {@inheritDoc} */
        @Override public Map<Object, Object> loadAll(Iterable<?> keys) {
            assertTrue(startCalled);

            assertNotNull(ignite);

            log.info("LoadAll: " + keys);

            ldrCallCnt.incrementAndGet();

            Map<Object, Object> loaded = new HashMap<>();

            for (Object key : keys) {
                Object val = storeMap.get(key);

                if (val != null)
                    loaded.put(key, val);
            }

            return loaded;
        }
    }

    /**
     *
     */
    class TestWriter implements CacheWriter<Integer, Integer>, LifecycleAware {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private boolean startCalled;

        /** {@inheritDoc} */
        @Override public void start() throws IgniteCheckedException {
            startCalled = true;

            assertNotNull(ignite);
        }

        /** {@inheritDoc} */
        @Override public void stop() throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> e) {
            assertTrue(startCalled);

            assertNotNull(ignite);

            log.info("Write: " + e);

            writerCallCnt.incrementAndGet();

            storeMap.put(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<? extends Integer, ? extends Integer>> entries) {
            assertTrue(startCalled);

            assertNotNull(ignite);

            log.info("WriteAll: " + entries);

            writerCallCnt.incrementAndGet();

            for (Cache.Entry<? extends Integer, ? extends Integer> e : entries)
                storeMap.put(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            assertTrue(startCalled);

            assertNotNull(ignite);

            log.info("Delete: " + key);

            writerCallCnt.incrementAndGet();

            storeMap.remove(key);
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) {
            assertTrue(startCalled);

            assertNotNull(ignite);

            log.info("DeleteAll: " + keys);

            writerCallCnt.incrementAndGet();

            for (Object key : keys)
                storeMap.remove(key);
        }
    }
}
