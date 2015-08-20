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

package org.apache.ignite.internal.processors.cache.portable;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.portable.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.portable.*;
import org.apache.ignite.portable.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;

import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.processor.*;
import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMemoryMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 * Test for portable objects stored in cache.
 */
public abstract class GridCachePortableObjectsAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    public static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int ENTRY_CNT = 100;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setCacheMode(cacheMode());
        cacheCfg.setAtomicityMode(atomicityMode());
        cacheCfg.setNearConfiguration(nearConfiguration());
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setCacheStoreFactory(singletonFactory(new TestStore()));
        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);
        cacheCfg.setLoadPreviousValue(true);
        cacheCfg.setBackups(1);

        if (offheapTiered()) {
            cacheCfg.setMemoryMode(OFFHEAP_TIERED);
            cacheCfg.setOffHeapMaxMemory(0);
        }

        cfg.setCacheConfiguration(cacheCfg);

        cfg.setMarshaller(new PortableMarshaller());

        return cfg;
    }

    /**
     * @return {@code True} if should use OFFHEAP_TIERED mode.
     */
    protected boolean offheapTiered() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < gridCount(); i++) {
            GridCacheAdapter<Object, Object> c = ((IgniteKernal)grid(i)).internalCache();

            for (GridCacheEntryEx e : c.map().entries0()) {
                Object key = e.key().value(c.context().cacheObjectContext(), false);
                Object val = CU.value(e.rawGet(), c.context(), false);

                if (key instanceof PortableObject)
                    assert ((PortableObjectImpl)key).detached() : val;

                if (val instanceof PortableObject)
                    assert ((PortableObjectImpl)val).detached() : val;
            }
        }

        IgniteCache<Object, Object> c = jcache(0);

        for (int i = 0; i < ENTRY_CNT; i++)
            c.remove(i);

        if (offheapTiered()) {
            for (int k = 0; k < 100; k++)
                c.remove(k);
        }

        assertEquals(0, c.size());
    }

    /**
     * @return Cache mode.
     */
    protected abstract CacheMode cacheMode();

    /**
     * @return Atomicity mode.
     */
    protected abstract CacheAtomicityMode atomicityMode();

    /**
     * @return Distribution mode.
     */
    protected abstract NearCacheConfiguration nearConfiguration();

    /**
     * @return Grid count.
     */
    protected abstract int gridCount();

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testCircularReference() throws Exception {
        IgniteCache c = keepPortableCache();

        TestReferenceObject obj1 = new TestReferenceObject();

        obj1.obj = new TestReferenceObject(obj1);

        c.put(1, obj1);

        PortableObject po = (PortableObject)c.get(1);

        String str = po.toString();

        log.info("toString: " + str);

        assertNotNull(str);

        assertTrue("Unexpected toString: " + str,
            str.startsWith("TestReferenceObject") && str.contains("obj=TestReferenceObject ["));

        TestReferenceObject obj1_r = po.deserialize();

        assertNotNull(obj1_r);

        TestReferenceObject obj2_r = obj1_r.obj;

        assertNotNull(obj2_r);

        assertSame(obj1_r, obj2_r.obj);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGet() throws Exception {
        IgniteCache<Integer, TestObject> c = jcache(0);

        for (int i = 0; i < ENTRY_CNT; i++)
            c.put(i, new TestObject(i));

        for (int i = 0; i < ENTRY_CNT; i++) {
            TestObject obj = c.get(i);

            assertEquals(i, obj.val);
        }

        IgniteCache<Integer, PortableObject> kpc = keepPortableCache();

        for (int i = 0; i < ENTRY_CNT; i++) {
            PortableObject po = kpc.get(i);

            assertEquals(i, (int)po.field("val"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIterator() throws Exception {
        IgniteCache<Integer, TestObject> c = jcache(0);

        Map<Integer, TestObject> entries = new HashMap<>();

        for (int i = 0; i < ENTRY_CNT; i++) {
            TestObject val = new TestObject(i);

            c.put(i, val);

            entries.put(i, val);
        }

        IgniteCache<Integer, PortableObject> prj = ((IgniteCacheProxy)c).keepPortable();

        Iterator<Cache.Entry<Integer, PortableObject>> it = prj.iterator();

        assertTrue(it.hasNext());

        while (it.hasNext()) {
            Cache.Entry<Integer, PortableObject> entry = it.next();

            assertTrue(entries.containsKey(entry.getKey()));

            TestObject o = entries.get(entry.getKey());

            PortableObject po = entry.getValue();

            assertEquals(o.val, (int)po.field("val"));

            entries.remove(entry.getKey());
        }

        assertEquals(0, entries.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollection() throws Exception {
        IgniteCache<Integer, Collection<TestObject>> c = jcache(0);

        for (int i = 0; i < ENTRY_CNT; i++) {
            Collection<TestObject> col = new ArrayList<>(3);

            for (int j = 0; j < 3; j++)
                col.add(new TestObject(i * 10 + j));

            c.put(i, col);
        }

        for (int i = 0; i < ENTRY_CNT; i++) {
            Collection<TestObject> col = c.get(i);

            assertEquals(3, col.size());

            Iterator<TestObject> it = col.iterator();

            for (int j = 0; j < 3; j++) {
                assertTrue(it.hasNext());

                assertEquals(i * 10 + j, it.next().val);
            }
        }

        IgniteCache<Integer, Collection<PortableObject>> kpc = keepPortableCache();

        for (int i = 0; i < ENTRY_CNT; i++) {
            Collection<PortableObject> col = kpc.get(i);

            assertEquals(3, col.size());

            Iterator<PortableObject> it = col.iterator();

            for (int j = 0; j < 3; j++) {
                assertTrue(it.hasNext());

                assertEquals(i * 10 + j, (int)it.next().field("val"));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMap() throws Exception {
        IgniteCache<Integer, Map<Integer, TestObject>> c = jcache(0);

        for (int i = 0; i < ENTRY_CNT; i++) {
            Map<Integer, TestObject> map = U.newHashMap(3);

            for (int j = 0; j < 3; j++) {
                int idx = i * 10 + j;

                map.put(idx, new TestObject(idx));
            }

            c.put(i, map);
        }

        for (int i = 0; i < ENTRY_CNT; i++) {
            Map<Integer, TestObject> map = c.get(i);

            assertEquals(3, map.size());

            for (int j = 0; j < 3; j++) {
                int idx = i * 10 + j;

                assertEquals(idx, map.get(idx).val);
            }
        }

        IgniteCache<Integer, Map<Integer, PortableObject>> kpc = keepPortableCache();

        for (int i = 0; i < ENTRY_CNT; i++) {
            Map<Integer, PortableObject> map = kpc.get(i);

            assertEquals(3, map.size());

            for (int j = 0; j < 3; j++) {
                int idx = i * 10 + j;

                assertEquals(idx, (int)map.get(idx).field("val"));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAsync() throws Exception {
        IgniteCache<Integer, TestObject> c = jcache(0);

        IgniteCache<Integer, TestObject> cacheAsync = c.withAsync();

        for (int i = 0; i < ENTRY_CNT; i++)
            c.put(i, new TestObject(i));

        for (int i = 0; i < ENTRY_CNT; i++) {
            cacheAsync.get(i);
            TestObject obj = cacheAsync.<TestObject>future().get();

            assertEquals(i, obj.val);
        }

        IgniteCache<Integer, PortableObject> kpc = keepPortableCache();

        IgniteCache<Integer, PortableObject> cachePortableAsync = kpc.withAsync();

        for (int i = 0; i < ENTRY_CNT; i++) {
            cachePortableAsync.get(i);

            PortableObject po = cachePortableAsync.<PortableObject>future().get();

            assertEquals(i, (int)po.field("val"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetTx() throws Exception {
        if (atomicityMode() != TRANSACTIONAL)
            return;

        IgniteCache<Integer, TestObject> c = jcache(0);

        for (int i = 0; i < ENTRY_CNT; i++)
            c.put(i, new TestObject(i));

        for (int i = 0; i < ENTRY_CNT; i++) {
            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                TestObject obj = c.get(i);

                assertEquals(i, obj.val);

                tx.commit();
            }
        }

        for (int i = 0; i < ENTRY_CNT; i++) {
            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                TestObject obj = c.get(i);

                assertEquals(i, obj.val);

                tx.commit();
            }
        }

        IgniteCache<Integer, PortableObject> kpc = keepPortableCache();

        for (int i = 0; i < ENTRY_CNT; i++) {
            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                PortableObject po = kpc.get(i);

                assertEquals(i, (int)po.field("val"));

                tx.commit();
            }
        }

        for (int i = 0; i < ENTRY_CNT; i++) {
            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                PortableObject po = kpc.get(i);

                assertEquals(i, (int)po.field("val"));

                tx.commit();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAsyncTx() throws Exception {
        if (atomicityMode() != TRANSACTIONAL)
            return;

        IgniteCache<Integer, TestObject> c = jcache(0);

        IgniteCache<Integer, TestObject> cacheAsync = c.withAsync();

        for (int i = 0; i < ENTRY_CNT; i++)
            c.put(i, new TestObject(i));

        for (int i = 0; i < ENTRY_CNT; i++) {
            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cacheAsync.get(i);

                TestObject obj = cacheAsync.<TestObject>future().get();

                assertEquals(i, obj.val);

                tx.commit();
            }
        }

        IgniteCache<Integer, PortableObject> kpc = keepPortableCache();
        IgniteCache<Integer, PortableObject> cachePortableAsync = kpc.withAsync();

        for (int i = 0; i < ENTRY_CNT; i++) {
            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cachePortableAsync.get(i);

                PortableObject po = cachePortableAsync.<PortableObject>future().get();

                assertEquals(i, (int)po.field("val"));

                tx.commit();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAll() throws Exception {
        IgniteCache<Integer, TestObject> c = jcache(0);

        for (int i = 0; i < ENTRY_CNT; i++)
            c.put(i, new TestObject(i));

        for (int i = 0; i < ENTRY_CNT; ) {
            Set<Integer> keys = new HashSet<>();

            for (int j = 0; j < 10; j++)
                keys.add(i++);

            Map<Integer, TestObject> objs = c.getAll(keys);

            assertEquals(10, objs.size());

            for (Map.Entry<Integer, TestObject> e : objs.entrySet())
                assertEquals(e.getKey().intValue(), e.getValue().val);
        }

        IgniteCache<Integer, PortableObject> kpc = keepPortableCache();

        for (int i = 0; i < ENTRY_CNT; ) {
            Set<Integer> keys = new HashSet<>();

            for (int j = 0; j < 10; j++)
                keys.add(i++);

            Map<Integer, PortableObject> objs = kpc.getAll(keys);

            assertEquals(10, objs.size());

            for (Map.Entry<Integer, PortableObject> e : objs.entrySet())
                assertEquals(new Integer(e.getKey().intValue()), e.getValue().field("val"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllAsync() throws Exception {
        IgniteCache<Integer, TestObject> c = jcache(0);

        IgniteCache<Integer, TestObject> cacheAsync = c.withAsync();

        for (int i = 0; i < ENTRY_CNT; i++)
            c.put(i, new TestObject(i));

        for (int i = 0; i < ENTRY_CNT; ) {
            Set<Integer> keys = new HashSet<>();

            for (int j = 0; j < 10; j++)
                keys.add(i++);

            cacheAsync.getAll(keys);

            Map<Integer, TestObject> objs = cacheAsync.<Map<Integer, TestObject>>future().get();

            assertEquals(10, objs.size());

            for (Map.Entry<Integer, TestObject> e : objs.entrySet())
                assertEquals(e.getKey().intValue(), e.getValue().val);
        }

        IgniteCache<Integer, PortableObject> kpc = keepPortableCache();
        IgniteCache<Integer, PortableObject> cachePortableAsync = kpc.withAsync();

        for (int i = 0; i < ENTRY_CNT; ) {
            Set<Integer> keys = new HashSet<>();

            for (int j = 0; j < 10; j++)
                keys.add(i++);


            cachePortableAsync.getAll(keys);

            Map<Integer, PortableObject> objs = cachePortableAsync.<Map<Integer, PortableObject>>future().get();

            assertEquals(10, objs.size());

            for (Map.Entry<Integer, PortableObject> e : objs.entrySet())
                assertEquals(new Integer(e.getKey().intValue()), e.getValue().field("val"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllTx() throws Exception {
        if (atomicityMode() != TRANSACTIONAL)
            return;

        IgniteCache<Integer, TestObject> c = jcache(0);

        for (int i = 0; i < ENTRY_CNT; i++)
            c.put(i, new TestObject(i));

        for (int i = 0; i < ENTRY_CNT; ) {
            Set<Integer> keys = new HashSet<>();

            for (int j = 0; j < 10; j++)
                keys.add(i++);

            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                Map<Integer, TestObject> objs = c.getAll(keys);

                assertEquals(10, objs.size());

                for (Map.Entry<Integer, TestObject> e : objs.entrySet())
                    assertEquals(e.getKey().intValue(), e.getValue().val);

                tx.commit();
            }

            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                Map<Integer, TestObject> objs = c.getAll(keys);

                assertEquals(10, objs.size());

                for (Map.Entry<Integer, TestObject> e : objs.entrySet())
                    assertEquals(e.getKey().intValue(), e.getValue().val);

                tx.commit();
            }
        }

        IgniteCache<Integer, PortableObject> kpc = keepPortableCache();

        for (int i = 0; i < ENTRY_CNT; ) {
            Set<Integer> keys = new HashSet<>();

            for (int j = 0; j < 10; j++)
                keys.add(i++);

            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                Map<Integer, PortableObject> objs = kpc.getAll(keys);

                assertEquals(10, objs.size());

                for (Map.Entry<Integer, PortableObject> e : objs.entrySet())
                    assertEquals(new Integer(e.getKey().intValue()), e.getValue().field("val"));

                tx.commit();
            }

            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                Map<Integer, PortableObject> objs = kpc.getAll(keys);

                assertEquals(10, objs.size());

                for (Map.Entry<Integer, PortableObject> e : objs.entrySet())
                    assertEquals(new Integer(e.getKey().intValue()), e.getValue().field("val"));

                tx.commit();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllAsyncTx() throws Exception {
        if (atomicityMode() != TRANSACTIONAL)
            return;

        IgniteCache<Integer, TestObject> c = jcache(0);
        IgniteCache<Integer, TestObject> cacheAsync = c.withAsync();

        for (int i = 0; i < ENTRY_CNT; i++)
            c.put(i, new TestObject(i));

        for (int i = 0; i < ENTRY_CNT; ) {
            Set<Integer> keys = new HashSet<>();

            for (int j = 0; j < 10; j++)
                keys.add(i++);

            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cacheAsync.getAll(keys);

                Map<Integer, TestObject> objs = cacheAsync.<Map<Integer, TestObject>>future().get();

                assertEquals(10, objs.size());

                for (Map.Entry<Integer, TestObject> e : objs.entrySet())
                    assertEquals(e.getKey().intValue(), e.getValue().val);

                tx.commit();
            }
        }

        IgniteCache<Integer, PortableObject> cache = keepPortableCache();

        for (int i = 0; i < ENTRY_CNT; ) {
            Set<Integer> keys = new HashSet<>();

            for (int j = 0; j < 10; j++)
                keys.add(i++);

            IgniteCache<Integer, PortableObject> asyncCache = cache.withAsync();

            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                asyncCache.getAll(keys);

                Map<Integer, PortableObject> objs = asyncCache.<Map<Integer, PortableObject>>future().get();

                assertEquals(10, objs.size());

                for (Map.Entry<Integer, PortableObject> e : objs.entrySet())
                    assertEquals(new Integer(e.getKey().intValue()), e.getValue().field("val"));

                tx.commit();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCache() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            jcache(i).localLoadCache(null);

        IgniteCache<Integer, TestObject> cache = jcache(0);

        assertEquals(3, cache.size(CachePeekMode.PRIMARY));

        assertEquals(1, cache.get(1).val);
        assertEquals(2, cache.get(2).val);
        assertEquals(3, cache.get(3).val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheAsync() throws Exception {
        for (int i = 0; i < gridCount(); i++) {
            IgniteCache<Object, Object> jcache = jcache(i).withAsync();

            jcache.loadCache(null);

            jcache.future().get();
        }

        IgniteCache<Integer, TestObject> cache = jcache(0);

        assertEquals(3, cache.size(CachePeekMode.PRIMARY));

        assertEquals(1, cache.get(1).val);
        assertEquals(2, cache.get(2).val);
        assertEquals(3, cache.get(3).val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheFilteredAsync() throws Exception {
        for (int i = 0; i < gridCount(); i++) {
            IgniteCache<Integer, TestObject> c = this.<Integer, TestObject>jcache(i).withAsync();

            c.loadCache(new P2<Integer, TestObject>() {
                @Override public boolean apply(Integer key, TestObject val) {
                    return val.val < 3;
                }
            });

            c.future().get();
        }

        IgniteCache<Integer, TestObject> cache = jcache(0);

        assertEquals(2, cache.size(CachePeekMode.PRIMARY));

        assertEquals(1, cache.get(1).val);
        assertEquals(2, cache.get(2).val);

        assertNull(cache.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransform() throws Exception {
        IgniteCache<Integer, PortableObject> c = keepPortableCache();

        checkTransform(primaryKey(c));

        if (cacheMode() != CacheMode.LOCAL) {
            checkTransform(backupKey(c));

            if (nearConfiguration() != null)
                checkTransform(nearKey(c));
        }
    }

    /**
     * @return Cache with keep portable flag.
     */
    private <K, V> IgniteCache<K, V> keepPortableCache() {
        return ignite(0).cache(null).withKeepPortable();
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkTransform(Integer key) throws Exception {
        log.info("Transform: " + key);

        IgniteCache<Integer, PortableObject> c = keepPortableCache();

        try {
            c.invoke(key, new EntryProcessor<Integer, PortableObject, Void>() {
                @Override public Void process(MutableEntry<Integer, PortableObject> e, Object... args) {
                    PortableObject val = e.getValue();

                    assertNull("Unexpected value: " + val, val);

                    return null;
                }
            });

            jcache(0).put(key, new TestObject(1));

            c.invoke(key, new EntryProcessor<Integer, PortableObject, Void>() {
                @Override public Void process(MutableEntry<Integer, PortableObject> e, Object... args) {
                    PortableObject val = e.getValue();

                    assertNotNull("Unexpected value: " + val, val);

                    assertEquals(new Integer(1), val.field("val"));

                    Ignite ignite = e.unwrap(Ignite.class);

                    IgnitePortables portables = ignite.portables();

                    PortableBuilder builder = portables.builder(val);

                    builder.setField("val", 2);

                    e.setValue(builder.build());

                    return null;
                }
            });

            PortableObject obj = c.get(key);

            assertEquals(new Integer(2), obj.field("val"));

            c.invoke(key, new EntryProcessor<Integer, PortableObject, Void>() {
                @Override public Void process(MutableEntry<Integer, PortableObject> e, Object... args) {
                    PortableObject val = e.getValue();

                    assertNotNull("Unexpected value: " + val, val);

                    assertEquals(new Integer(2), val.field("val"));

                    e.setValue(val);

                    return null;
                }
            });

            obj = c.get(key);

            assertEquals(new Integer(2), obj.field("val"));

            c.invoke(key, new EntryProcessor<Integer, PortableObject, Void>() {
                @Override public Void process(MutableEntry<Integer, PortableObject> e, Object... args) {
                    PortableObject val = e.getValue();

                    assertNotNull("Unexpected value: " + val, val);

                    assertEquals(new Integer(2), val.field("val"));

                    e.remove();

                    return null;
                }
            });

            assertNull(c.get(key));
        }
        finally {
            c.remove(key);
        }
    }

    /**
     *
     */
    private static class TestObject implements PortableMarshalAware {
        /** */
        private int val;

        /**
         */
        private TestObject() {
            // No-op.
        }

        /**
         * @param val Value.
         */
        private TestObject(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(PortableWriter writer) throws PortableException {
            writer.writeInt("val", val);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(PortableReader reader) throws PortableException {
            val = reader.readInt("val");
        }
    }

    /**
     *
     */
    private static class TestReferenceObject implements PortableMarshalAware {
        /** */
        private TestReferenceObject obj;

        /**
         */
        private TestReferenceObject() {
            // No-op.
        }

        /**
         * @param obj Object.
         */
        private TestReferenceObject(TestReferenceObject obj) {
            this.obj = obj;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(PortableWriter writer) throws PortableException {
            writer.writeObject("obj", obj);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(PortableReader reader) throws PortableException {
            obj = reader.readObject("obj");
        }
    }

    /**
     *
     */
    private static class TestStore extends CacheStoreAdapter<Integer, Object> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, Object> clo, Object... args) {
            for (int i = 1; i <= 3; i++)
                clo.apply(i, new TestObject(i));
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object load(Integer key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ?> e) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op.
        }
    }
}
