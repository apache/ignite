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

package org.apache.ignite.internal.processors.cache.binary;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryArrayIdentityResolver;
import org.apache.ignite.binary.BinaryNameMapper;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryIdentityResolver;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.BinaryObjectOffheapImpl;
import org.apache.ignite.binary.BinaryFieldIdentityResolver;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.MapCacheStoreStrategy;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test for binary objects stored in cache.
 */
public abstract class GridCacheBinaryObjectsAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    public static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int ENTRY_CNT = 100;

    /** */
    private static IgniteConfiguration cfg;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        CacheConfiguration cacheCfg = createCacheConfig();

        cacheCfg.setCacheStoreFactory(singletonFactory(new TestStore()));

        CacheConfiguration binKeysCacheCfg = createCacheConfig();

        binKeysCacheCfg.setCacheStoreFactory(singletonFactory(new MapCacheStoreStrategy.MapCacheStore()));
        binKeysCacheCfg.setStoreKeepBinary(true);
        binKeysCacheCfg.setName("BinKeysCache");

        cfg.setCacheConfiguration(cacheCfg, binKeysCacheCfg);
        cfg.setMarshaller(new BinaryMarshaller());

        List<BinaryTypeConfiguration> binTypes = new ArrayList<>();

        binTypes.add(new BinaryTypeConfiguration() {{
            setTypeName("ArrayHashedKey");

            setIdentityResolver(new BinaryArrayIdentityResolver());
        }});

        binTypes.add(new BinaryTypeConfiguration() {{
            setTypeName("FieldsHashedKey");

            BinaryFieldIdentityResolver id = new BinaryFieldIdentityResolver();
            id.setFieldNames("fld1", "fld3");

            setIdentityResolver(id);
        }});

        binTypes.add(new BinaryTypeConfiguration() {{
            setTypeName("CustomHashedKey");

            setIdentityResolver(new IdentityResolver());
        }});

        binTypes.add(new BinaryTypeConfiguration() {{
            setTypeName(ComplexBinaryFieldsListHashedKey.class.getName());

            BinaryFieldIdentityResolver id = new BinaryFieldIdentityResolver();

            id.setFieldNames("secondField", "thirdField");

            setIdentityResolver(id);
        }});

        BinaryConfiguration binCfg = new BinaryConfiguration();
        binCfg.setTypeConfigurations(binTypes);

        cfg.setBinaryConfiguration(binCfg);

        CacheKeyConfiguration arrayHashCfg = new CacheKeyConfiguration("ArrayHashedKey", "fld1");
        CacheKeyConfiguration fieldsHashCfg = new CacheKeyConfiguration("FieldsHashedKey", "fld1");
        CacheKeyConfiguration customHashCfg = new CacheKeyConfiguration("CustomHashedKey", "fld1");

        cfg.setCacheKeyConfiguration(arrayHashCfg, fieldsHashCfg, customHashCfg);

        GridCacheBinaryObjectsAbstractSelfTest.cfg = cfg;

        return cfg;
    }

    /**
     * @return Cache configuration with basic settings.
     */
    @SuppressWarnings("unchecked")
    private CacheConfiguration createCacheConfig() {
        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setCacheMode(cacheMode());
        cacheCfg.setAtomicityMode(atomicityMode());
        cacheCfg.setNearConfiguration(nearConfiguration());
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);
        cacheCfg.setLoadPreviousValue(true);
        cacheCfg.setBackups(1);

        if (offheapTiered()) {
            cacheCfg.setMemoryMode(OFFHEAP_TIERED);
            cacheCfg.setOffHeapMaxMemory(0);
        }

        return cacheCfg;
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

            for (GridCacheEntryEx e : c.map().entries()) {
                Object key = e.key().value(c.context().cacheObjectContext(), false);
                Object val = CU.value(e.rawGet(), c.context(), false);

                if (key instanceof BinaryObject)
                    assert ((BinaryObjectImpl)key).detached() : val;

                if (val instanceof BinaryObject)
                    assert ((BinaryObjectImpl)val).detached() : val;
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
        IgniteCache c = keepBinaryCache();

        TestReferenceObject obj1 = new TestReferenceObject();

        obj1.obj = new TestReferenceObject(obj1);

        c.put(1, obj1);

        BinaryObject po = (BinaryObject)c.get(1);

        String str = po.toString();

        log.info("toString: " + str);

        assertNotNull(str);

        BinaryNameMapper nameMapper = BinaryContext.defaultNameMapper();

        if (cfg.getBinaryConfiguration() != null && cfg.getBinaryConfiguration().getNameMapper() != null)
            nameMapper = cfg.getBinaryConfiguration().getNameMapper();

        String typeName = nameMapper.typeName(TestReferenceObject.class.getName());

        assertTrue("Unexpected toString: " + str,
            S.INCLUDE_SENSITIVE ?
            str.startsWith(typeName) && str.contains("obj=" + typeName + " [") :
            str.startsWith("BinaryObject") && str.contains("idHash=") && str.contains("hash=")
        );

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

        IgniteCache<Integer, BinaryObject> kpc = keepBinaryCache();

        for (int i = 0; i < ENTRY_CNT; i++) {
            BinaryObject po = kpc.get(i);

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

        IgniteCache<Integer, BinaryObject> prj = ((IgniteCacheProxy)c).keepBinary();

        Iterator<Cache.Entry<Integer, BinaryObject>> it = prj.iterator();

        assertTrue(it.hasNext());

        while (it.hasNext()) {
            Cache.Entry<Integer, BinaryObject> entry = it.next();

            assertTrue(entries.containsKey(entry.getKey()));

            TestObject o = entries.get(entry.getKey());

            BinaryObject po = entry.getValue();

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

        IgniteCache<Integer, Collection<BinaryObject>> kpc = keepBinaryCache();

        for (int i = 0; i < ENTRY_CNT; i++) {
            Collection<BinaryObject> col = kpc.get(i);

            assertEquals(3, col.size());

            Iterator<BinaryObject> it = col.iterator();

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

        IgniteCache<Integer, Map<Integer, BinaryObject>> kpc = keepBinaryCache();

        for (int i = 0; i < ENTRY_CNT; i++) {
            Map<Integer, BinaryObject> map = kpc.get(i);

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

            assertNotNull(obj);

            assertEquals(i, obj.val);
        }

        IgniteCache<Integer, BinaryObject> kpc = keepBinaryCache();

        IgniteCache<Integer, BinaryObject> cacheBinaryAsync = kpc.withAsync();

        for (int i = 0; i < ENTRY_CNT; i++) {
            cacheBinaryAsync.get(i);

            BinaryObject po = cacheBinaryAsync.<BinaryObject>future().get();

            assertEquals(i, (int)po.field("val"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testBasicArrays() throws Exception {
        IgniteCache<Integer, Object> cache = jcache(0);

        checkArrayClass(cache, new String[] {"abc"});

        checkArrayClass(cache, new byte[] {1});

        checkArrayClass(cache, new short[] {1});

        checkArrayClass(cache, new int[] {1});

        checkArrayClass(cache, new long[] {1});

        checkArrayClass(cache, new float[] {1});

        checkArrayClass(cache, new double[] {1});

        checkArrayClass(cache, new char[] {'a'});

        checkArrayClass(cache, new boolean[] {false});

        checkArrayClass(cache, new UUID[] {UUID.randomUUID()});

        checkArrayClass(cache, new Date[] {new Date()});

        checkArrayClass(cache, new Timestamp[] {new Timestamp(System.currentTimeMillis())});

        checkArrayClass(cache, new BigDecimal[] {new BigDecimal(100)});
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomArrays() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-3244");

        IgniteCache<Integer, TestObject[]> cache = jcache(0);

        for (int i = 0; i < ENTRY_CNT; i++) {
            TestObject[] arr = new TestObject[] {new TestObject(i)};

            cache.put(0, arr);
        }


        for (int i = 0; i < ENTRY_CNT; i++) {
            TestObject[] obj = cache.get(i);

            assertEquals(1, obj.length);
            assertEquals(i, obj[0].val);
        }
    }

    /**
     * @param cache Ignite cache.
     * @param arr Array to check.
     */
    private void checkArrayClass(IgniteCache<Integer, Object> cache, Object arr) {
        cache.put(0, arr);

        Object res = cache.get(0);

        assertEquals(arr.getClass(), res.getClass());
        GridTestUtils.deepEquals(arr, res);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetTx1() throws Exception {
        checkGetTx(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetTx2() throws Exception {
        checkGetTx(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     */
    private void checkGetTx(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        if (atomicityMode() != TRANSACTIONAL)
            return;

        IgniteCache<Integer, TestObject> c = jcache(0);
        IgniteCache<Integer, BinaryObject> kbCache = keepBinaryCache();

        for (int i = 0; i < ENTRY_CNT; i++)
            c.put(i, new TestObject(i));

        for (int i = 0; i < ENTRY_CNT; i++) {
            try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                TestObject obj = c.get(i);

                assertEquals(i, obj.val);

                tx.commit();
            }
        }

        for (int i = 0; i < ENTRY_CNT; i++) {
            try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                BinaryObject val = kbCache.get(i);

                assertFalse("Key=" + i, val instanceof BinaryObjectOffheapImpl);

                assertEquals(i, (int)val.field("val"));

                kbCache.put(i, val);

                tx.commit();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetTxAsync1() throws Exception {
        checkGetAsyncTx(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetTxAsync2() throws Exception {
        checkGetAsyncTx(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     */
    private void checkGetAsyncTx(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        if (atomicityMode() != TRANSACTIONAL)
            return;

        IgniteCache<Integer, TestObject> c = jcache(0);
        IgniteCache<Integer, TestObject> cAsync = c.withAsync();
        IgniteCache<Integer, BinaryObject> kbCache = keepBinaryCache();
        IgniteCache<Integer, BinaryObject> kbCacheAsync = kbCache.withAsync();

        for (int i = 0; i < ENTRY_CNT; i++)
            c.put(i, new TestObject(i));

        for (int i = 0; i < ENTRY_CNT; i++) {
            try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                cAsync.get(i);

                TestObject obj = (TestObject)cAsync.future().get();

                assertEquals(i, obj.val);

                tx.commit();
            }
        }

        for (int i = 0; i < ENTRY_CNT; i++) {
            try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                kbCacheAsync.get(i);

                BinaryObject val = (BinaryObject)kbCacheAsync.future().get();

                assertFalse("Key=" + i, val instanceof BinaryObjectOffheapImpl);

                assertEquals(i, (int)val.field("val"));

                kbCacheAsync.put(i, val);

                kbCacheAsync.future().get();

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

        IgniteCache<Integer, BinaryObject> kpc = keepBinaryCache();
        IgniteCache<Integer, BinaryObject> cacheBinaryAsync = kpc.withAsync();

        for (int i = 0; i < ENTRY_CNT; i++) {
            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cacheBinaryAsync.get(i);

                BinaryObject po = cacheBinaryAsync.<BinaryObject>future().get();

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

        IgniteCache<Integer, BinaryObject> kpc = keepBinaryCache();

        for (int i = 0; i < ENTRY_CNT; ) {
            Set<Integer> keys = new HashSet<>();

            for (int j = 0; j < 10; j++)
                keys.add(i++);

            Map<Integer, BinaryObject> objs = kpc.getAll(keys);

            assertEquals(10, objs.size());

            for (Map.Entry<Integer, BinaryObject> e : objs.entrySet())
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

        IgniteCache<Integer, BinaryObject> kpc = keepBinaryCache();
        IgniteCache<Integer, BinaryObject> cacheBinaryAsync = kpc.withAsync();

        for (int i = 0; i < ENTRY_CNT; ) {
            Set<Integer> keys = new HashSet<>();

            for (int j = 0; j < 10; j++)
                keys.add(i++);

            cacheBinaryAsync.getAll(keys);

            Map<Integer, BinaryObject> objs = cacheBinaryAsync.<Map<Integer, BinaryObject>>future().get();

            assertEquals(10, objs.size());

            for (Map.Entry<Integer, BinaryObject> e : objs.entrySet())
                assertEquals(new Integer(e.getKey().intValue()), e.getValue().field("val"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllTx1() throws Exception {
        checkGetAllTx(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllTx2() throws Exception {
        checkGetAllTx(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     */
    private void checkGetAllTx(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        if (atomicityMode() != TRANSACTIONAL)
            return;

        IgniteCache<Integer, TestObject> c = jcache(0);
        IgniteCache<Integer, BinaryObject> kpc = keepBinaryCache();

        for (int i = 0; i < ENTRY_CNT; i++)
            c.put(i, new TestObject(i));

        for (int i = 0; i < ENTRY_CNT; ) {
            Set<Integer> keys = new HashSet<>();

            for (int j = 0; j < 10; j++)
                keys.add(i++);

            try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                Map<Integer, TestObject> objs = c.getAll(keys);

                assertEquals(10, objs.size());

                for (Map.Entry<Integer, TestObject> e : objs.entrySet())
                    assertEquals(e.getKey().intValue(), e.getValue().val);

                tx.commit();
            }
        }

        for (int i = 0; i < ENTRY_CNT; ) {
            Set<Integer> keys = new HashSet<>();

            for (int j = 0; j < 10; j++)
                keys.add(i++);

            try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                Map<Integer, BinaryObject> objs = kpc.getAll(keys);

                assertEquals(10, objs.size());

                for (Map.Entry<Integer, BinaryObject> e : objs.entrySet()) {
                    BinaryObject val = e.getValue();

                    assertEquals(new Integer(e.getKey().intValue()), val.field("val"));

                    kpc.put(e.getKey(), val);

                    assertFalse("Key=" + i, val instanceof BinaryObjectOffheapImpl);
                }

                tx.commit();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllAsyncTx1() throws Exception {
        checkGetAllAsyncTx(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllAsyncTx2() throws Exception {
        checkGetAllAsyncTx(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     */
    private void checkGetAllAsyncTx(TransactionConcurrency concurrency, TransactionIsolation isolation) {
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

            try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                cacheAsync.getAll(keys);

                Map<Integer, TestObject> objs = cacheAsync.<Map<Integer, TestObject>>future().get();

                assertEquals(10, objs.size());

                for (Map.Entry<Integer, TestObject> e : objs.entrySet())
                    assertEquals(e.getKey().intValue(), e.getValue().val);

                tx.commit();
            }
        }

        IgniteCache<Integer, BinaryObject> cache = keepBinaryCache();

        for (int i = 0; i < ENTRY_CNT; ) {
            Set<Integer> keys = new HashSet<>();

            for (int j = 0; j < 10; j++)
                keys.add(i++);

            IgniteCache<Integer, BinaryObject> asyncCache = cache.withAsync();

            try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                asyncCache.getAll(keys);

                Map<Integer, BinaryObject> objs = asyncCache.<Map<Integer, BinaryObject>>future().get();

                assertEquals(10, objs.size());

                for (Map.Entry<Integer, BinaryObject> e : objs.entrySet()) {
                    BinaryObject val = e.getValue();

                    assertEquals(new Integer(e.getKey().intValue()), val.field("val"));

                    assertFalse("Key=" + e.getKey(), val instanceof BinaryObjectOffheapImpl);
                }

                tx.commit();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({ "ThrowableResultOfMethodCallIgnored", "unchecked" })
    public void testPutWithoutHashCode() throws Exception {
        final IgniteCache c = jcache(0);

        GridCacheAdapter<Object, Object> cache0 = grid(0).context().cache().internalCache(null);

        cache0.forceKeyCheck();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                c.put(new TestObject(5), 5);
                return null;
            }
        }, IllegalArgumentException.class, "Cache key must override hashCode() and equals() methods: ");

        BinaryObjectBuilder bldr = grid(0).binary().builder(TestObject.class.getName());
        bldr.setField("val", 5);

        final BinaryObject binKey = bldr.build();

        cache0.forceKeyCheck();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                c.put(binKey, 5);
                return null;
            }
        }, IllegalArgumentException.class, "Cache key created with BinaryBuilder is missing hash code - " +
            "please set it explicitly during building by using BinaryBuilder.hashCode(int)");
    }

    /**
     *
     */
    @SuppressWarnings("unchecked")
    public void testCrossFormatObjectsIdentity() {
        IgniteCache c = binKeysCache();

        c.put(new ComplexBinaryFieldsListHashedKey(), "zzz");

        // Now let's build an identical key for get
        BinaryObjectBuilder bldr = grid(0).binary().builder(ComplexBinaryFieldsListHashedKey.class.getName());

        bldr.setField("firstField", 365);
        bldr.setField("secondField", "value");
        bldr.setField("thirdField", 0x1020304050607080L);

        BinaryObject binKey = bldr.build();

        assertEquals("zzz", c.get(binKey));
    }

    /**
     *
     */
    @SuppressWarnings("unchecked")
    public void testPutWithArrayHashing() {
        IgniteCache c = binKeysCache();

        {
            BinaryObjectBuilder bldr = grid(0).binary().builder("ArrayHashedKey");

            BinaryObject binKey = bldr.setField("fld1", 5).setField("fld2", 1).setField("fld3", "abc").build();

            c.put(binKey, "zzz");
        }

        // Now let's build an identical key for get.
        {
            BinaryObjectBuilder bldr = grid(0).binary().builder("ArrayHashedKey");

            BinaryObject binKey = bldr.setField("fld1", 5).setField("fld2", 1).setField("fld3", "abc").build();

            assertEquals("zzz", c.get(binKey));
        }

        // Now let's build not identical key for get.
        {
            BinaryObjectBuilder bldr = grid(0).binary().builder("ArrayHashedKey");

            BinaryObject binKey = bldr.setField("fld1", 5).setField("fld2", 100).setField("fld3", "abc").build();

            assertNull(c.get(binKey));
        }
    }

    /**
     *
     */
    @SuppressWarnings("unchecked")
    public void testPutWithFieldsHashing() {
        IgniteCache c = binKeysCache();

        {
            BinaryObjectBuilder bldr = grid(0).binary().builder("FieldsHashedKey");

            bldr.setField("fld1", 5);
            bldr.setField("fld2", 1);
            bldr.setField("fld3", "abc");

            BinaryObject binKey = bldr.build();

            c.put(binKey, "zzz");
        }

        // Now let's build an identical key for get
        {
            BinaryObjectBuilder bldr = grid(0).binary().builder("FieldsHashedKey");

            bldr.setField("fld1", 5);
            bldr.setField("fld2", 100); // This one does not participate in hashing
            bldr.setField("fld3", "abc");

            BinaryObject binKey = bldr.build();

            assertEquals("zzz", c.get(binKey));
        }
    }

    /**
     *
     */
    @SuppressWarnings("unchecked")
    public void testPutWithCustomHashing() {
        IgniteCache c = binKeysCache();

        {
            BinaryObjectBuilder bldr = grid(0).binary().builder("CustomHashedKey");

            bldr.setField("fld1", 5);
            bldr.setField("fld2", "abc");

            BinaryObject binKey = bldr.build();

            c.put(binKey, "zzz");
        }

        // Now let's build an identical key for get
        {
            BinaryObjectBuilder bldr = grid(0).binary().builder("CustomHashedKey");

            bldr.setField("fld1", 5);
            bldr.setField("fld2", "xxx");

            BinaryObject binKey = bldr.build();

            assertEquals("zzz", c.get(binKey));
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testKeepBinaryTxOverwrite() throws Exception {
        if (atomicityMode() != TRANSACTIONAL)
            return;

        IgniteCache<Integer, TestObject> cache = ignite(0).cache(null);

        cache.put(0, new TestObject(1));

        for (TransactionConcurrency conc : TransactionConcurrency.values()) {
            for (TransactionIsolation iso : TransactionIsolation.values()) {
                try (Transaction tx = ignite(0).transactions().txStart(conc, iso)) {
                    cache.withKeepBinary().get(0);

                    cache.invoke(0, new ObjectEntryProcessor());

                    tx.commit();
                }
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
        IgniteCache<Integer, BinaryObject> c = keepBinaryCache();

        checkTransform(primaryKey(c));

        if (cacheMode() != CacheMode.LOCAL) {
            checkTransform(backupKey(c));

            if (nearConfiguration() != null)
                checkTransform(nearKey(c));
        }
    }

    /**
     * @return Cache with keep binary flag.
     */
    private <K, V> IgniteCache<K, V> keepBinaryCache() {
        return ignite(0).cache(null).withKeepBinary();
    }

    /**
     * @return Cache tuned to utilize classless binary objects as keys.
     */
    private <K, V> IgniteCache<K, V> binKeysCache() {
        return ignite(0).cache("BinKeysCache").withKeepBinary();
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkTransform(Integer key) throws Exception {
        log.info("Transform: " + key);

        IgniteCache<Integer, BinaryObject> c = keepBinaryCache();

        try {
            c.invoke(key, new EntryProcessor<Integer, BinaryObject, Void>() {
                @Override public Void process(MutableEntry<Integer, BinaryObject> e, Object... args) {
                    BinaryObject val = e.getValue();

                    assertNull("Unexpected value: " + val, val);

                    return null;
                }
            });

            jcache(0).put(key, new TestObject(1));

            c.invoke(key, new EntryProcessor<Integer, BinaryObject, Void>() {
                @Override public Void process(MutableEntry<Integer, BinaryObject> e, Object... args) {
                    BinaryObject val = e.getValue();

                    assertNotNull("Unexpected value: " + val, val);

                    assertEquals(new Integer(1), val.field("val"));

                    Ignite ignite = e.unwrap(Ignite.class);

                    IgniteBinary binaries = ignite.binary();

                    BinaryObjectBuilder builder = binaries.builder(val);

                    builder.setField("val", 2);

                    e.setValue(builder.build());

                    return null;
                }
            });

            BinaryObject obj = c.get(key);

            assertEquals(new Integer(2), obj.field("val"));

            c.invoke(key, new EntryProcessor<Integer, BinaryObject, Void>() {
                @Override public Void process(MutableEntry<Integer, BinaryObject> e, Object... args) {
                    BinaryObject val = e.getValue();

                    assertNotNull("Unexpected value: " + val, val);

                    assertEquals(new Integer(2), val.field("val"));

                    e.setValue(val);

                    return null;
                }
            });

            obj = c.get(key);

            assertEquals(new Integer(2), obj.field("val"));

            c.invoke(key, new EntryProcessor<Integer, BinaryObject, Void>() {
                @Override public Void process(MutableEntry<Integer, BinaryObject> e, Object... args) {
                    BinaryObject val = e.getValue();

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
    private static class TestObject implements Binarylizable {
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
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeInt("val", val);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            val = reader.readInt("val");
        }
    }

    /**
     * No-op entry processor.
     */
    private static class ObjectEntryProcessor implements EntryProcessor<Integer, TestObject, Boolean> {
        @Override
        public Boolean process(MutableEntry<Integer, TestObject> entry, Object... args) throws EntryProcessorException {
            TestObject obj = entry.getValue();

            entry.setValue(new TestObject(obj.val));

            return true;
        }
    }

    /**
     *
     */
    private static class TestReferenceObject implements Binarylizable {
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
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeObject("obj", obj);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
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

    /**
     *
     */
    private final static class IdentityResolver implements BinaryIdentityResolver {
        /** {@inheritDoc} */
        @Override public int hashCode(BinaryObject builder) {
            return (Integer) builder.field("fld1") * 31 / 5;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(BinaryObject o1, BinaryObject o2) {
            return o1 == o2 || (o1 != null && o2 != null && F.eq(o1.field("fld1"), o2.field("fld1")));

        }
    }

    /**
     * Key to test puts and gets with
     */
    @SuppressWarnings({"ConstantConditions", "unused"})
    private final static class ComplexBinaryFieldsListHashedKey {
        /** */
        private final Integer firstField = 1;

        /** */
        private final String secondField = "value";

        /** */
        private final Long thirdField = 0x1020304050607080L;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ComplexBinaryFieldsListHashedKey that = (ComplexBinaryFieldsListHashedKey) o;

            return secondField.equals(that.secondField) &&
                thirdField.equals(that.thirdField);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = secondField.hashCode();
            res = 31 * res + thirdField.hashCode();
            return res;
        }
    }
}
