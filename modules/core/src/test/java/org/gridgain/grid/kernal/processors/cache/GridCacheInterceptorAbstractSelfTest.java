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

import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.*;
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
        return true; // TODO: 8429 tests with store enabled.
    }

    private void assertCancelInvokeCount(Operation op) {
        // TODO 8429.
    }

    private void assertInvokeCount(Operation op) {
        // TODO 8429.
    }

    /**
     * @throws Exception If failed.
     */
    public void testCancelUpdate() throws Exception {
        for (Operation op : Operation.values()) {
            testCancelUpdate(primaryKey(0), op);

            afterTest();

            if (cacheMode() != LOCAL) {
                testCancelUpdate(backupKey(0), op);

                afterTest();
            }
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
    private void testCancelUpdate(String key, Operation op) throws Exception {
        // Interceptor returns null to disabled update.
        GridCacheInterceptor retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public Object onBeforePut(Object key, @Nullable Object oldVal, Object newVal) {
                return null;
            }
        };

        interceptor.retInterceptor = retInterceptor;

        // Execute update when value is null, it should not change cache value.

        log.info("Update 1 " + op);

        update(0, op, key, 1, null);

        checkCacheValue(key, null);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforePutMap.size());

        GridBiTuple t = interceptor.beforePutMap.get(key);

        assertEquals(null, t.get1());
        assertEquals(1, t.get2());

        assertCancelInvokeCount(op);

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

        update(0, op, key, 2, 1);

        checkCacheValue(key, 1);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforePutMap.size());

        t = interceptor.beforePutMap.get(key);

        assertEquals(1, t.get1());
        assertEquals(2, t.get2());

        assertCancelInvokeCount(op);
    }

    /**
     * @throws Exception If failed.
     */
    public void testModifyUpdate() throws Exception {
        for (Operation op : Operation.values()) {
            testModifyUpdate(primaryKey(0), op);

            afterTest();

            if (cacheMode() != LOCAL) {
                testModifyUpdate(backupKey(0), op);

                afterTest();
            }
        }
    }

    /**
     * @param key Key.
     * @param op Operation type.
     * @throws Exception If failed.
     */
    private void testModifyUpdate(String key, Operation op) throws Exception {
        // Interceptor returns incremented new value.
        GridCacheInterceptor retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public Object onBeforePut(Object key, @Nullable Object oldVal, Object newVal) {
                return (Integer)newVal + 1;
            }
        };

        // Execute update when value is null.

        interceptor.retInterceptor = retInterceptor;

        log.info("Update 1 " + op);

        update(0, op, key, 1, null);

        checkCacheValue(key, 2);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforePutMap.size());

        GridBiTuple t = interceptor.beforePutMap.get(key);

        assertEquals(null, t.get1());
        assertEquals(1, t.get2());

        assertEquals(1, interceptor.afterPutMap.size());

        assertEquals(2, interceptor.afterPutMap.get(key));

        assertInvokeCount(op);

        // Execute update when value is not null.

        interceptor.reset();

        interceptor.retInterceptor = retInterceptor;

        log.info("Update 2 " + op);

        update(0, op, key, 3, 2);

        checkCacheValue(key, 4);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforePutMap.size());

        t = interceptor.beforePutMap.get(key);

        assertEquals(2, t.get1());
        assertEquals(3, t.get2());

        assertEquals(1, interceptor.afterPutMap.size());

        assertEquals(4, interceptor.afterPutMap.get(key));

        assertInvokeCount(op);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCancelRemove() throws Exception {
        for (Operation op : Operation.values()) {
            testCancelRemove(primaryKey(0), op);

            afterTest();

            if (cacheMode() != LOCAL) {
                testCancelRemove(backupKey(0), op);

                afterTest();
            }
        }
    }

    /**
     * @param key Key.
     * @param op Operation type.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void testCancelRemove(String key, Operation op) throws Exception {
        // Interceptor disables remove and returns null.
        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public GridBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new GridBiTuple(true, null);
            }
        };

        // Execute remove when value is null.

        log.info("Remove 1 " + op);

        remove(0, op, key, null, null);

        checkCacheValue(key, null);

        // Check values passed to interceptor.

        assertEquals(0, interceptor.beforeRemoveMap.size());

        assertEquals(null, interceptor.beforeRemoveMap.get(key));

        assertCancelInvokeCount(op);

        log.info("Remove 2 " + op);

        interceptor.reset();

        // Interceptor disables remove and changes return value.
        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public GridBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new GridBiTuple(true, 900);
            }
        };

        // Execute remove when value is null, interceptor changes return value.

        remove(0, op, key, null, 900);

        checkCacheValue(key, null);

        // Check values passed to interceptor.

        assertEquals(0, interceptor.beforeRemoveMap.size());

        assertEquals(null, interceptor.beforeRemoveMap.get(key));

        assertCancelInvokeCount(op);

        // Disable interceptor and update cache.

        interceptor.reset();

        interceptor.disabled = true;

        clearCaches();

        cache(0).put(key, 1);

        checkCacheValue(key, 1);

        // Execute remove when value is not null, it should not change cache value.

        interceptor.reset();

        interceptor.disabled = false;

        // Interceptor disables remove and returns null.
        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public GridBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new GridBiTuple(true, null);
            }
        };

        log.info("Remove 3 " + op);

        remove(0, op, key, 1, null);

        checkCacheValue(key, 1);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforeRemoveMap.size());

        assertEquals(1, interceptor.beforeRemoveMap.get(key));

        assertCancelInvokeCount(op);

        interceptor.reset();

        // Interceptor disables remove and changes return value.
        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public GridBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new GridBiTuple(true, 1000);
            }
        };

        log.info("Remove 4 " + op);

        remove(0, op, key, 1, 1000);

        checkCacheValue(key, 1);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforeRemoveMap.size());

        assertEquals(1, interceptor.beforeRemoveMap.get(key));

        assertCancelInvokeCount(op);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemove() throws Exception {
        for (Operation op : Operation.values()) {
            testRemove(primaryKey(0), op);

            afterTest();

            if (cacheMode() != LOCAL) {
                testRemove(backupKey(0), op);

                afterTest();
            }
        }
    }

    /**
     * @param key Key.
     * @param op Operation type.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void testRemove(String key, Operation op) throws Exception {
        // Interceptor changes return value to null.
        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public GridBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new GridBiTuple(false, null);
            }
        };

        // Execute remove when value is null.

        log.info("Remove 1 " + op);

        remove(0, op, key, null, null);

        checkCacheValue(key, null);

        // Check values passed to interceptor.

        assertEquals(0, interceptor.beforeRemoveMap.size());

        assertEquals(0, interceptor.afterRemoveMap.size());

        assertInvokeCount(op);

        log.info("Remove 2 " + op);

        interceptor.reset();

        // Interceptor changes return value.
        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public GridBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new GridBiTuple(false, 900);
            }
        };

        // Execute remove when value is null.

        remove(0, op, key, null, 900);

        checkCacheValue(key, null);

        // Check values passed to interceptor.

        assertEquals(0, interceptor.beforeRemoveMap.size());

        assertEquals(0, interceptor.afterRemoveMap.size());

        assertInvokeCount(op);

        // Disable interceptor and update cache.

        interceptor.reset();

        interceptor.disabled = true;

        clearCaches();

        cache(0).put(key, 1);

        checkCacheValue(key, 1);

        // Execute remove when value is not null.

        interceptor.reset();

        interceptor.disabled = false;

        // Interceptor changes return value to null.
        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public GridBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new GridBiTuple(false, null);
            }
        };

        log.info("Remove 3 " + op);

        remove(0, op, key, 1, null);

        checkCacheValue(key, null);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforeRemoveMap.size());

        assertEquals(1, interceptor.beforeRemoveMap.get(key));

        assertEquals(1, interceptor.afterRemoveMap.size());

        assertEquals(1, interceptor.afterRemoveMap.get(key));

        assertInvokeCount(op);

        // Disable interceptor and update cache.

        interceptor.disabled = true;

        clearCaches();

        cache(0).put(key, 2);

        checkCacheValue(key, 2);

        // Execute remove when value is not null.

        interceptor.reset();

        interceptor.disabled = false;

        // Interceptor changes return value.
        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public GridBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new GridBiTuple(false, 1000);
            }
        };

        log.info("Remove 4 " + op);

        remove(0, op, key, 2, 1000);

        checkCacheValue(key, null);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforeRemoveMap.size());

        assertEquals(2, interceptor.beforeRemoveMap.get(key));

        assertEquals(1, interceptor.afterRemoveMap.size());

        assertEquals(2, interceptor.afterRemoveMap.get(key));

        assertInvokeCount(op);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBatchUpdate() throws Exception {
        testBatchUpdate(Operation.UPDATE);

        afterTest();

        testBatchUpdate(Operation.TRANSFORM);
    }

    /**
     * @param op Operation type.
     * @throws Exception If failed.
     */
    private void testBatchUpdate(Operation op) throws Exception {
        // Interceptor returns incremented new value.
        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public Object onBeforePut(Object key, @Nullable Object oldVal, Object newVal) {
                return (Integer)newVal + 1;
            }
        };

        Map<String, Integer> map = new HashMap<>();

        final String key1;
        String key2;
        String key3;

        if (cacheMode() == LOCAL) {
            key1 = "1";
            key2 = "2";
            key3 = "3";
        }
        else {
            List<String> keys = primaryKeys(0, 2);
            key1 = keys.get(0);
            key2 = keys.get(1);
            key3 = backupKey(0);
        }

        map.put(key1, 1);
        map.put(key2, 2);
        map.put(key3, 3);

        log.info("Batch update 1: " + op);

        batchUpdate(0, op, map);

        checkCacheValue(key1, 2);
        checkCacheValue(key2, 3);
        checkCacheValue(key3, 4);

        assertEquals(3, interceptor.beforePutMap.size());

        assertBeforePutValue(key1, null, 1);
        assertBeforePutValue(key2, null, 2);
        assertBeforePutValue(key3, null, 3);

        assertEquals(3, interceptor.afterPutMap.size());

        assertEquals(2, interceptor.afterPutMap.get(key1));
        assertEquals(3, interceptor.afterPutMap.get(key2));
        assertEquals(4, interceptor.afterPutMap.get(key3));

        interceptor.reset();

        // Interceptor returns incremented new value, cancels update for one key.
        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public Object onBeforePut(Object key, @Nullable Object oldVal, Object newVal) {
                if (key.equals(key1))
                    return null;

                return (Integer)newVal + 1;
            }
        };

        map.put(key1, 100);
        map.put(key2, 200);
        map.put(key3, 300);

        log.info("Batch update 2: " + op);

        batchUpdate(0, op, map);

        checkCacheValue(key1, 2);
        checkCacheValue(key2, 201);
        checkCacheValue(key3, 301);

        assertEquals(3, interceptor.beforePutMap.size());

        assertBeforePutValue(key1, 2, 100);
        assertBeforePutValue(key2, 3, 200);
        assertBeforePutValue(key3, 4, 300);

        assertEquals(2, interceptor.afterPutMap.size());

        assertEquals(201, interceptor.afterPutMap.get(key2));
        assertEquals(301, interceptor.afterPutMap.get(key3));
    }

    /**
     * @throws Exception If failed.
     */
    public void testBatchRemove() throws Exception {
        testBatchRemove(Operation.UPDATE);

        afterTest();

        testBatchRemove(Operation.TRANSFORM);
    }

    /**
     * @param op Operation type.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void testBatchRemove(Operation op) throws Exception {
        Map<String, Integer> map = new HashMap<>();

        final String key1;
        String key2;
        String key3;

        if (cacheMode() == LOCAL) {
            key1 = "1";
            key2 = "2";
            key3 = "3";
        }
        else {
            List<String> keys = primaryKeys(0, 2);
            key1 = keys.get(0);
            key2 = keys.get(1);
            key3 = backupKey(0);
        }

        map.put(key1, 1);
        map.put(key2, 2);
        map.put(key3, 3);

        // Interceptor does not cancel update.
        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public GridBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new GridBiTuple(false, 999);
            }
        };


        log.info("Batch remove 1: " + op);

        batchRemove(0, op, map);

        checkCacheValue(key1, null);
        checkCacheValue(key2, null);
        checkCacheValue(key3, null);

        assertEquals(0, interceptor.beforeRemoveMap.size());

        assertEquals(0, interceptor.afterRemoveMap.size());

        // Disable interceptor and put some values in cache.

        interceptor.disabled = true;

        cache(0).putAll(map);

        interceptor.disabled = false;

        interceptor.reset();

        // Interceptor does not cancel update.
        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public GridBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new GridBiTuple(false, 999);
            }
        };

        log.info("Batch remove 2: " + op);

        batchRemove(0, op, map);

        checkCacheValue(key1, null);
        checkCacheValue(key2, null);
        checkCacheValue(key3, null);

        assertEquals(3, interceptor.beforeRemoveMap.size());

        assertEquals(1, interceptor.beforeRemoveMap.get(key1));
        assertEquals(2, interceptor.beforeRemoveMap.get(key2));
        assertEquals(3, interceptor.beforeRemoveMap.get(key3));

        assertEquals(3, interceptor.afterRemoveMap.size());

        assertEquals(1, interceptor.afterRemoveMap.get(key1));
        assertEquals(2, interceptor.afterRemoveMap.get(key2));
        assertEquals(3, interceptor.afterRemoveMap.get(key3));

        // Disable interceptor and put some values in cache.

        interceptor.disabled = true;

        cache(0).putAll(map);

        interceptor.disabled = false;

        interceptor.reset();

        // Interceptor cancels update for one key.
        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable@Override public GridBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new GridBiTuple(key.equals(key1), 999);
            }
        };

        log.info("Batch remove 3: " + op);

        batchRemove(0, op, map);

        checkCacheValue(key1, 1);
        checkCacheValue(key2, null);
        checkCacheValue(key3, null);

        assertEquals(3, interceptor.beforeRemoveMap.size());

        assertEquals(1, interceptor.beforeRemoveMap.get(key1));
        assertEquals(2, interceptor.beforeRemoveMap.get(key2));
        assertEquals(3, interceptor.beforeRemoveMap.get(key3));

        assertEquals(2, interceptor.afterRemoveMap.size());

        assertEquals(2, interceptor.afterRemoveMap.get(key2));
        assertEquals(3, interceptor.afterRemoveMap.get(key3));
    }

    /**
     * @param key Key.
     * @param oldVal Expected old value.
     * @param newVal Expected new value.
     */
    private void assertBeforePutValue(String key, @Nullable Object oldVal, @Nullable Object newVal) {
        GridBiTuple t = interceptor.beforePutMap.get(key);

        assertNotNull(t);
        assertEquals(t.get1(), oldVal);
        assertEquals(t.get2(), newVal);
    }

    /**
     * @param grid Grid index.
     * @param op Operation type.
     * @param key Key.
     * @param val Value.
     * @param expOld Expected expOld value.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void update(int grid, Operation op, String key, final Integer val, @Nullable final Integer expOld)
        throws Exception {
        cacheUpdate(grid, false, op, key, val, expOld, null);
    }

    /**
     * @param grid Grid index.
     * @param op Operation type.
     * @param key Key.
     * @param expOld Expected expOld value.
     * @param expRmvRet Expected remove result.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void remove(int grid, Operation op, String key, @Nullable final Integer expOld,
        @Nullable final Integer expRmvRet) throws Exception {
        cacheUpdate(grid, true, op, key, null, expOld, expRmvRet);
    }

    /**
     * @param grid Grid index.
     * @param rmv If {@code true} then executes remove.
     * @param op Operation type.
     * @param key Key.
     * @param val Value.
     * @param expOld Expected expOld value.
     * @param expRmvRet Expected remove result.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void cacheUpdate(int grid, boolean rmv, Operation op, String key, final Integer val,
        @Nullable final Integer expOld, @Nullable final Integer expRmvRet)
        throws Exception {
        GridCache<String, Integer> cache = cache(grid);

        if (rmv) {
            assertNull(val);

            switch (op) {
                case UPDATE: {
                    assertEquals(expRmvRet, cache.remove(key));

                    break;
                }

                case UPDATEX: {
                    cache.removex(key);

                    break;
                }

                case UPDATE_FILTER: {
                    Object old = cache.remove(key, new GridPredicate<GridCacheEntry<String, Integer>>() {
                        @Override public boolean apply(GridCacheEntry<String, Integer> entry) {
                            return true;
                        }
                    });

                    assertEquals(expRmvRet, old);

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
                    Object old = cache.put(key, val, new P1<GridCacheEntry<String, Integer>>() {
                        @Override public boolean apply(GridCacheEntry<String, Integer> entry) {
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
     * @param grid Grid index.
     * @param op Operation type.
     * @param map Key/values map.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void batchUpdate(int grid, Operation op, final Map<String, Integer> map) throws Exception {
        cacheBatchUpdate(grid, false, op, map);
    }

    /**
     * @param grid Grid index.
     * @param op Operation type.
     * @param map Key/values map.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void batchRemove(int grid, Operation op, final Map<String, Integer> map) throws Exception {
        cacheBatchUpdate(grid, true, op, map);
    }

    /**
     * @param grid Grid index.
     * @param rmv If {@code true} then executes remove.
     * @param op Operation type.
     * @param map Key/values map.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void cacheBatchUpdate(int grid, boolean rmv, Operation op, final Map<String, Integer> map)
        throws Exception {
        GridCache<String, Integer> cache = cache(grid);

        if (rmv) {
            switch (op) {
                case UPDATE: {
                    cache.removeAll(map.keySet());

                    break;
                }

                case TRANSFORM: {
                    cache.transformAll(map.keySet(), new GridClosure<Integer, Integer>() {
                        @Nullable
                        @Override
                        public Integer apply(Integer old) {
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
                    cache.putAll(map);

                    break;
                }

                case TRANSFORM: {
                    Map<String, GridClosure<Integer, Integer>> m = new HashMap<>();

                    for (final String key : map.keySet()) {
                        m.put(key, new GridClosure<Integer, Integer>() {
                            @Override public Integer apply(Integer old) {
                                return map.get(key);
                            }
                        });
                    }

                    cache.transformAll(m);

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
        return primaryKeys(idx, 1).get(0);
    }

    /**
     * @param idx Grid index.
     * @param cnt Number of keys.
     * @return Primary keys for grid.
     */
    private List<String> primaryKeys(int idx, int cnt) {
        assert cnt > 0;

        GridCacheAffinity aff = cache(0).affinity();

        List<String> keys = new ArrayList<>(cnt);

        for (int i = 0; i < 10_000; i++) {
            String key = String.valueOf(i);

            if (aff.isPrimary(grid(idx).localNode(), key)) {
                keys.add(key);

                if (keys.size() == cnt)
                    break;
            }
        }

        assertEquals(cnt, keys.size());

        return keys;
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

            assertNotNull(retInterceptor);

            GridBiTuple ret = retInterceptor.onBeforeRemove(key, val);

            log.info("Before remove [key=" + key + ", val=" + val + ", ret=" + ret + ']');

            invokeCnt.incrementAndGet();

            if (val != null) {
                Object old = beforeRemoveMap.put(key, val);

                if (old != null)
                    assertEquals(old, val);
            }

            return ret;
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Object key, Object val) {
            if (disabled)
                return;

            log.info("After remove [key=" + key + ", val=" + val + ']');

            invokeCnt.incrementAndGet();

            if (val != null) {
                Object old = afterRemoveMap.put(key, val);

                if (old != null)
                    assertEquals(old, val);
            }
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
