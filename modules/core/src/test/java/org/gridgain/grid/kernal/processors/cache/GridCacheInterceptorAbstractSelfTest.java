/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.PRIMARY;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Tests {@link GridCacheInterceptor}.
 */
public abstract class GridCacheInterceptorAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static Interceptor interceptor;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        interceptor = new Interceptor();

        super.beforeTestsStarted();

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        interceptor.reset();

        interceptor.disabled = true;

        super.afterTest();

        interceptor.disabled = false;

        assertEquals(0, interceptor.invokeCnt.get());
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration  ccfg = super.cacheConfiguration(gridName);

        assertNotNull(interceptor);

        ccfg.setInterceptor(interceptor);

        if (ccfg.getAtomicityMode() == GridCacheAtomicityMode.ATOMIC) {
            assertNotNull(writeOrderMode());

            ccfg.setAtomicWriteOrderMode(writeOrderMode());
        }

        if (!storeEnabled())
            ccfg.setStore(null);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @return Atomic cache write order mode.
     */
    @Nullable protected GridCacheAtomicWriteOrderMode writeOrderMode() {
        return null;
    }

    /**
     * @return {@code True} if cache store is enabled.
     */
    protected boolean storeEnabled() {
        return false; // TODO: 8429 tests with store enabled.
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgnoreUpdate() throws Exception {
        for (Operation op : Operation.values()) {
            testIgnore(primaryKey(0), op);

            afterTest();

            testIgnore(backupKey(0), op);

            afterTest();
        }
    }

    /**
     * @param op Operation type.
     * @return {@code True} if this is atomic cache and update is first run on primary node.
     */
    private int expectedIgnoreInvokeCount(Operation op) {
        int dataNodes = cacheMode() == REPLICATED ? gridCount() : 2;

        if (atomicityMode() == GridCacheAtomicityMode.TRANSACTIONAL)
            return dataNodes + (storeEnabled() ? 1 : 0); // One call before store is updated.
        else {
            // If update goes through primary node and it is cancelled then backups aren't updated.
            return (writeOrderMode() == PRIMARY ||
                (op == Operation.TRANSFORM || op == Operation.UPDATE_FILTER)) ? 1 : dataNodes;
        }
    }

    /**
     * @param op Operation type.
     * @return {@code True} if this is atomic cache and update is first run on primary node.
     */
    private int expectedInvokeCount(Operation op) {
        int dataNodes = cacheMode() == REPLICATED ? gridCount() : 2;

        if (atomicityMode() == GridCacheAtomicityMode.TRANSACTIONAL)
            // Update + after update + one call before store is updated.
            return dataNodes * 2 + (storeEnabled() ? 1 : 0);
        else {
            return (writeOrderMode() == PRIMARY ||
                (op == Operation.TRANSFORM || op == Operation.UPDATE_FILTER)) ? 2 : dataNodes * 2;
        }
    }

    /**
     * @param key Key.
     * @param op Operation type.
     * @throws Exception If failed.
     */
    private void testIgnore(String key, Operation op) throws Exception {
        int expInvokeCnt = expectedIgnoreInvokeCount(op);

        // Interceptor returns null to disabled update.
        GridCacheInterceptor retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public Object onBeforePut(Object key, @Nullable Object oldVal, Object newVal) {
                return null;
            }
        };

        interceptor.retInterceptor = retInterceptor;

        // Execute update when value is null, it should not change cache value.

        log.info("Update 1 " + op);

        update(0, false, op, key, 1, null);

        checkCacheValue(key, null);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforePutMap.size());

        GridBiTuple t = interceptor.beforePutMap.get(key);

        assertEquals(null, t.get1());
        assertEquals(1, t.get2());

        assertEquals(expInvokeCnt, interceptor.invokeCnt.get());

        // Disable interceptor and update cache.

        interceptor.reset();

        interceptor.disabled = true;

        clearCaches();

        cache(0).put(key, 1);

        checkCacheValue(key, 1);

        // Execute update when value is not null, it should not change cache value.

        interceptor.disabled = false;
        interceptor.retInterceptor = retInterceptor;

        log.info("Update 2 " + op);

        update(0, false, op, key, 2, 1);

        checkCacheValue(key, 1);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforePutMap.size());

        t = interceptor.beforePutMap.get(key);

        assertEquals(1, t.get1());
        assertEquals(2, t.get2());

        assertEquals(expInvokeCnt, interceptor.invokeCnt.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testModifyUpdate() throws Exception {
        for (Operation op : Operation.values()) {
            testModifyUpdate(primaryKey(0), op);

            afterTest();

            testModifyUpdate(backupKey(0), op);

            afterTest();
        }
    }

    /**
     * @param key Key.
     * @param op Operation type.
     * @throws Exception If failed.
     */
    private void testModifyUpdate(String key, Operation op) throws Exception {
        int expInvokeCnt = expectedInvokeCount(op);

        // Interceptor returns incremented new value.
        GridCacheInterceptor retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public Object onBeforePut(Object key, @Nullable Object oldVal, Object newVal) {
                return (Integer)newVal + 1;
            }
        };

        // Execute update when value is null.

        interceptor.retInterceptor = retInterceptor;

        log.info("Update 1 " + op);

        update(0, false, op, key, 1, null);

        checkCacheValue(key, 2);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforePutMap.size());

        GridBiTuple t = interceptor.beforePutMap.get(key);

        assertEquals(null, t.get1());
        assertEquals(1, t.get2());

        assertEquals(1, interceptor.afterPutMap.size());

        assertEquals(2, interceptor.afterPutMap.get(key));

        assertEquals(expInvokeCnt, interceptor.invokeCnt.get());

        // Execute update when value is not null.

        interceptor.reset();

        interceptor.retInterceptor = retInterceptor;

        log.info("Update 2 " + op);

        update(0, false, op, key, 3, 2);

        checkCacheValue(key, 4);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforePutMap.size());

        t = interceptor.beforePutMap.get(key);

        assertEquals(2, t.get1());
        assertEquals(3, t.get2());

        assertEquals(1, interceptor.afterPutMap.size());

        assertEquals(4, interceptor.afterPutMap.get(key));

        assertEquals(expInvokeCnt, interceptor.invokeCnt.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgnoreRemove() throws Exception {

    }

    /**
     * @param key Key.
     * @param op Operation type.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void testIgnoreRemove(String key, Operation op) throws Exception {
        int expInvokeCnt = expectedIgnoreInvokeCount(op);

        // Interceptor returns null to disabled update.
        GridCacheInterceptor retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public GridBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new GridBiTuple(true, null);
            }
        };

        interceptor.retInterceptor = retInterceptor;

        // Execute remove when value is null.

        log.info("Update 1 " + op);

        update(0, true, op, key, null, null);


    }

    /**
     * @param grid Grid index.
     * @param rmv If {@code true} then executes remove.
     * @param op Operation type.
     * @param key Key.
     * @param val Value.
     * @param expOld Expected expOld value.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void update(int grid, boolean rmv, Operation op, String key, final Integer val,
        @Nullable final Integer expOld)
        throws Exception {
        GridCache cache = cache(grid);

        if (rmv) {
            assertNull(val);

            switch (op) {
                case UPDATE: {
                    assertEquals(expOld, cache.remove(key));

                    break;
                }

                case UPDATEX: {
                    cache.removex(key);

                    break;
                }

                case UPDATE_FILTER: {
                    Object old = cache.remove(key, new P1<GridCacheEntry>() {
                        @Override public boolean apply(GridCacheEntry entry) {
                            return true;
                        }
                    });

                    assertEquals(expOld, old);

                    break;
                }

                case TRANSFORM: {
                    cache.transform(key, new GridClosure<Integer, Integer>() {
                        @Nullable @Override public Integer apply(Integer old) {
                            assertEquals(expOld, old);

                            return null;
                        }
                    });

                    break;
                }

                default:
                    fail();
            }
        }
        else {
            switch (op) {
                case UPDATE: {
                    assertEquals(expOld, cache.put(key, val));

                    break;
                }

                case UPDATEX: {
                    cache.putx(key, val);

                    break;
                }

                case UPDATE_FILTER: {
                    Object old = cache.put(key, val, new P1<GridCacheEntry>() {
                        @Override public boolean apply(GridCacheEntry entry) {
                            return true;
                        }
                    });

                    assertEquals(expOld, old);

                    break;
                }

                case TRANSFORM: {
                    cache.transform(key, new GridClosure<Integer, Integer>() {
                        @Override public Integer apply(Integer old) {
                            assertEquals(expOld, old);

                            return val;
                        }
                    });

                    break;
                }

                default:
                    fail();
            }
        }
    }

    /**
     * @param idx Grid index.
     * @return Primary key for grid.
     */
    private String primaryKey(int idx) {
        GridCacheAffinity aff = cache(0).affinity();

        String key = null;

        for (int i = 0; i < 10_000; i++) {
            if (aff.isPrimary(grid(idx).localNode(), String.valueOf(i))) {
                key = String.valueOf(i);

                break;
            }
        }

        assertNotNull(key);

        return key;
    }

    /**
     * @param idx Grid index.
     * @return Primary key for grid.
     */
    private String backupKey(int idx) {
        GridCacheAffinity aff = cache(0).affinity();

        String key = null;

        for (int i = 0; i < 10_000; i++) {
            if (aff.isBackup(grid(idx).localNode(), String.valueOf(i))) {
                key = String.valueOf(i);

                break;
            }
        }

        assertNotNull(key);

        return key;
    }

    /**
     * @param key Key.
     * @param expVal Expected value.
     * @throws Exception If failed.
     */
    private void checkCacheValue(Object key, @Nullable Object expVal) throws Exception {
        interceptor.disabled = true;

        if (storeEnabled())
            assertEquals("Unexpected store value", expVal, map.get(key));

        try {
            for (int i = 0; i < gridCount(); i++)
                assertEquals("Unexpected value for grid " + i, expVal, grid(i).cache(null).get(key));
        }
        finally {
            interceptor.disabled = false;
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void clearCaches() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            cache(i).removeAll();
    }

    /**
     *
     */
    private static class InterceptorAdapter implements GridCacheInterceptor {
        /** */
        @Nullable @Override public Object onGet(Object key, Object val) {
            fail("onGet not expected");

            return null;
        }

        /** */
        @Nullable @Override public Object onBeforePut(Object key, @Nullable Object oldVal, Object newVal) {
            fail("onBeforePut not expected");

            return null;
        }

        /** */
        @Override public void onAfterPut(Object key, Object val) {
            fail("onAfterPut not expected");
        }

        /** */
        @Nullable @Override public GridBiTuple onBeforeRemove(Object key, @Nullable Object val) {
            fail("onBeforeRemove not expected");

            return null;
        }

        /** */
        @Override public void onAfterRemove(Object key, Object val) {
            fail("onAfterRemove not expected");
        }
    }

    /**
     *
     */
    private enum Operation {
        /**
         *
         */
        UPDATE,

        /**
         *
         */
        UPDATEX,

        /**
         *
         */
        TRANSFORM,

        /**
         *
         */
        UPDATE_FILTER
    }

    /**
     *
     */
    private class Interceptor implements GridCacheInterceptor {
        /** */
        private final Map<Object, Object> getMap = new ConcurrentHashMap8<>();

        /** */
        private final Map<Object, Object> afterPutMap = new ConcurrentHashMap8<>();

        /** */
        private final Map<Object, GridBiTuple> beforePutMap = new ConcurrentHashMap8<>();

        /** */
        private final Map<Object, Object> beforeRemoveMap = new ConcurrentHashMap8<>();

        /** */
        private final Map<Object, Object> afterRemoveMap = new ConcurrentHashMap8<>();

        /** */
        private final AtomicInteger invokeCnt = new AtomicInteger();

        /** */
        private volatile boolean disabled;

        /** */
        private volatile GridCacheInterceptor retInterceptor;

        /** {@inheritDoc} */
        @Nullable @Override public Object onGet(Object key, Object val) {
            if (disabled)
                return val;

            log.info("Get [key=" + key + ", val=" + val + ']');

            getMap.put(key, val);

            return null;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Nullable @Override public Object onBeforePut(Object key, @Nullable Object oldVal, Object newVal) {
            if (disabled)
                return newVal;

            assertNotNull(retInterceptor);

            Object ret = retInterceptor.onBeforePut(key, oldVal, newVal);

            log.info("Before put [key=" + key + ", oldVal=" + oldVal + ", newVal=" + newVal + ", ret=" + ret + ']');

            invokeCnt.incrementAndGet();

            GridBiTuple t = beforePutMap.put(key, new GridBiTuple(oldVal, newVal));

            if (t != null) {
                assertEquals(t.get1(), oldVal);
                assertEquals(t.get2(), newVal);
            }

            return ret;
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Object key, Object val) {
            if (disabled)
                return;

            log.info("After put [key=" + key + ", val=" + val + ']');

            invokeCnt.incrementAndGet();

            Object old = afterPutMap.put(key, val);

            if (old != null)
                assertEquals(old, val);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override @Nullable public GridBiTuple onBeforeRemove(Object key, @Nullable Object val) {
            if (disabled)
                return new GridBiTuple(false, val);

            log.info("Before remove [key=" + key + ", val=" + val + ']');

            invokeCnt.incrementAndGet();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Object key, Object val) {
            if (disabled)
                return;

            log.info("After remove [key=" + key + ", val=" + val + ']');

            invokeCnt.incrementAndGet();

            Object old = afterRemoveMap.put(key, val);

            if (old != null)
                assertEquals(old, val);
        }

        /**
         *
         */
        public void reset() {
            invokeCnt.set(0);

            getMap.clear();
            beforePutMap.clear();
            afterPutMap.clear();
            afterRemoveMap.clear();
            beforeRemoveMap.clear();

            retInterceptor = null;
        }
    }
}
