/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.portables.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;
import org.junit.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMemoryMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 *
 */
public abstract class GridCacheOffHeapTieredAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return PARTITIONED_ONLY;
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        if (portableEnabled()) {
            GridPortableConfiguration pCfg = new GridPortableConfiguration();

            pCfg.setClassNames(Arrays.asList(TestValue.class.getName()));

            cfg.setPortableConfiguration(pCfg);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setAtomicWriteOrderMode(PRIMARY);

        ccfg.setMemoryMode(OFFHEAP_TIERED);
        ccfg.setOffHeapMaxMemory(1024 * 1024);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransform() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        checkTransform(primaryKey(cache));

        checkTransform(backupKey(cache));

        checkTransform(nearKey(cache));
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkTransform(Integer key) throws Exception {
        GridCache<Integer, Integer> c = grid(0).cache(null);

        c.transform(key, new C1<Integer, Integer>() {
            @Override public Integer apply(Integer val) {
                assertNull("Unexpected value: " + val, val);

                return null;
            }
        });

        c.putx(key, 1);

        c.transform(key, new C1<Integer, Integer>() {
            @Override public Integer apply(Integer val) {
                assertNotNull("Unexpected value: " + val, val);

                assertEquals((Integer) 1, val);

                return val + 1;
            }
        });

        assertEquals((Integer)2, c.get(key));

        c.transform(key, new C1<Integer, Integer>() {
            @Override public Integer apply(Integer val) {
                assertNotNull("Unexpected value: " + val, val);

                assertEquals((Integer)2, val);

                return val;
            }
        });

        assertEquals((Integer) 2, c.get(key));

        c.transform(key, new C1<Integer, Integer>() {
            @Override
            public Integer apply(Integer val) {
                assertNotNull("Unexpected value: " + val, val);

                assertEquals((Integer) 2, val);

                return null;
            }
        });

        assertNull(c.get(key));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetRemove() throws Exception {
        GridCache<Integer, Integer> c = grid(0).cache(null);

        checkPutGetRemove(primaryKey(c));

        checkPutGetRemove(backupKey(c));

        checkPutGetRemove(nearKey(c));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetRemoveByteArray() throws Exception {
        GridCache<Integer, Integer> c = grid(0).cache(null);

        checkPutGetRemoveByteArray(primaryKey(c));

        checkPutGetRemoveByteArray(backupKey(c));

        checkPutGetRemoveByteArray(nearKey(c));
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkPutGetRemove(Integer key) throws Exception {
        GridCache<Integer, Integer> c = grid(0).cache(null);

        checkValue(key, null);

        assertNull(c.put(key, key));

        checkValue(key, key);

        assertEquals(key, c.remove(key));

        checkValue(key, null);

        if (atomicityMode() == TRANSACTIONAL) {
            checkPutGetRemoveTx(key, PESSIMISTIC);

            checkPutGetRemoveTx(key, OPTIMISTIC);
        }
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkPutGetRemoveByteArray(Integer key) throws Exception {
        GridCache<Integer, byte[]> c = grid(0).cache(null);

        checkValue(key, null);

        byte[] val = new byte[] {key.byteValue()};

        assertNull(c.put(key, val));

        checkValue(key, val);

        Assert.assertArrayEquals(val, c.remove(key));

        checkValue(key, null);

        if (atomicityMode() == TRANSACTIONAL) {
            checkPutGetRemoveTxByteArray(key, PESSIMISTIC);

            checkPutGetRemoveTxByteArray(key, OPTIMISTIC);
        }
    }

    /**
     * @param key Key,
     * @param txConcurrency Transaction concurrency.
     * @throws Exception If failed.
     */
    private void checkPutGetRemoveTx(Integer key, GridCacheTxConcurrency txConcurrency) throws Exception {
        GridCache<Integer, Integer> c = grid(0).cache(null);

        GridCacheTx tx = c.txStart(txConcurrency, REPEATABLE_READ);

        assertNull(c.put(key, key));

        tx.commit();

        checkValue(key, key);

        tx = c.txStart(txConcurrency, REPEATABLE_READ);

        assertEquals(key, c.remove(key));

        tx.commit();

        checkValue(key, null);
    }

    /**
     * @param key Key,
     * @param txConcurrency Transaction concurrency.
     * @throws Exception If failed.
     */
    private void checkPutGetRemoveTxByteArray(Integer key, GridCacheTxConcurrency txConcurrency) throws Exception {
        GridCache<Integer, byte[]> c = grid(0).cache(null);

        GridCacheTx tx = c.txStart(txConcurrency, REPEATABLE_READ);

        byte[] val = new byte[] {key.byteValue()};

        assertNull(c.put(key, val));

        tx.commit();

        checkValue(key, val);

        tx = c.txStart(txConcurrency, REPEATABLE_READ);

        Assert.assertArrayEquals(val, c.remove(key));

        tx.commit();

        checkValue(key, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPromote() throws Exception {
        GridCache<Integer, TestValue> c = grid(0).cache(null);

        TestValue val = new TestValue(new byte[100 * 1024]);

        List<Integer> keys = primaryKeys(c, 200);

        for (Integer key : keys)
            c.putx(key, val);

        for (int i = 0; i < 50; i++) {
            TestValue val0 = c.promote(keys.get(i));

            Assert.assertArrayEquals(val.val, val0.val);
        }

        List<Integer> keys0 = keys.subList(50, 100);

        c.promoteAll(keys0);

        for (Integer key : keys0) {
            TestValue val0 = c.get(key);

            Assert.assertArrayEquals(val.val, val0.val);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAllRemoveAll() throws Exception {
        Map<Integer, Integer> map = new HashMap<>();

        for (int i = 0; i < 100; i++)
            map.put(i, i);

        GridCache<Integer, Integer> c = grid(0).cache(null);

        Map<Integer, Integer> map0 = c.getAll(map.keySet());

        assertTrue(map0.isEmpty());

        c.putAll(map);

        map0 = c.getAll(map.keySet());

        assertEquals(map, map0);

        for (Map.Entry<Integer, Integer> e : map.entrySet())
            checkValue(e.getKey(), e.getValue());

        c.transformAll(map.keySet(), new C1<Integer, Integer>() {
            @Override public Integer apply(Integer val) {
                return val + 1;
            }
        });

        map0 = c.getAll(map.keySet());

        for (Map.Entry<Integer, Integer> e : map0.entrySet())
            assertEquals((Integer)(e.getKey() + 1), e.getValue());

        for (Map.Entry<Integer, Integer> e : map.entrySet())
            checkValue(e.getKey(), e.getValue() + 1);

        c.removeAll(map.keySet());

        map0 = c.getAll(map.keySet());

        assertTrue(map0.isEmpty());

        for (Map.Entry<Integer, Integer> e : map.entrySet())
            checkValue(e.getKey(), null);

        if (atomicityMode() == TRANSACTIONAL) {
            checkPutAllGetAllRemoveAllTx(PESSIMISTIC);

            checkPutAllGetAllRemoveAllTx(OPTIMISTIC);
        }
    }

    /**
     * @param txConcurrency Transaction concurrency.
     * @throws Exception If failed.
     */
    private void checkPutAllGetAllRemoveAllTx(GridCacheTxConcurrency txConcurrency) throws Exception {
        Map<Integer, Integer> map = new HashMap<>();

        for (int i = 0; i < 100; i++)
            map.put(i, i);

        GridCache<Integer, Integer> c = grid(0).cache(null);

        Map<Integer, Integer> map0 = c.getAll(map.keySet());

        assertTrue(map0.isEmpty());

        try (GridCacheTx tx = c.txStart(txConcurrency, REPEATABLE_READ)) {
            c.putAll(map);

            tx.commit();
        }

        map0 = c.getAll(map.keySet());

        assertEquals(map, map0);

        for (Map.Entry<Integer, Integer> e : map.entrySet())
            checkValue(e.getKey(), e.getValue());

        try (GridCacheTx tx = c.txStart(txConcurrency, REPEATABLE_READ)) {
            c.removeAll(map.keySet());

            tx.commit();
        }

        map0 = c.getAll(map.keySet());

        assertTrue(map0.isEmpty());

        for (Map.Entry<Integer, Integer> e : map.entrySet())
            checkValue(e.getKey(), null);
    }

    /**
     * @param key Key.
     * @param val Value.
     * @throws Exception If failed.
     */
    private void checkValue(Object key, @Nullable Object val) throws Exception {
        for (int i = 0; i < gridCount(); i++) {
            if (val != null && val.getClass() == byte[].class) {
                Assert.assertArrayEquals("Unexpected value for grid: " + i,
                    (byte[]) val,
                    (byte[]) grid(0).cache(null).get(key));
            }
            else
                assertEquals("Unexpected value for grid: " + i, val, grid(0).cache(null).get(key));
        }
    }

    /**
     *
     */
    public static class TestValue {
        /** */
        @SuppressWarnings("PublicField")
        public byte[] val;

        /**
         * Default constructor.
         */
        public TestValue() {
            // No-op.
        }

        /**
         * @param val Value.
         */
        public TestValue(byte[] val) {
            this.val = val;
        }
    }
}
