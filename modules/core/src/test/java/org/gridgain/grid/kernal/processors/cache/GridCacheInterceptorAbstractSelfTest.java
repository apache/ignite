/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;

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

    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionsConfiguration().setTxSerializableEnabled(true);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration  ccfg = super.cacheConfiguration(gridName);

        assertNotNull(interceptor);

        ccfg.setInterceptor(interceptor);

        if (ccfg.getAtomicityMode() == ATOMIC) {
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
        return false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGet() throws Exception {
        testGet(primaryKey(0));

        afterTest();

        if (cacheMode() != LOCAL)
            testGet(backupKey(0));
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void testGet(String key) throws Exception {
        // Try when value is not in cache.

        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public Object onGet(Object key, Object val) {
                return null;
            }
        };

        log.info("Get 1.");

        assertEquals(null, cache(0).get(key));

        assertEquals(1, interceptor.invokeCnt.get());

        assertEquals(0, interceptor.getMap.size());

        interceptor.reset();

        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public Object onGet(Object key, Object val) {
                return 1;
            }
        };

        log.info("Get 2.");

        assertEquals((Integer)1, cache(0).get(key));

        assertEquals(1, interceptor.invokeCnt.get());

        assertEquals(0, interceptor.getMap.size());

        interceptor.reset();

        // Disable interceptor and update cache.

        interceptor.disabled = true;

        cache(0).put(key, 100);

        interceptor.disabled = false;

        // Try when value is in cache.

        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public Object onGet(Object key, Object val) {
                return null;
            }
        };

        log.info("Get 3.");

        assertEquals(null, cache(0).get(key));

        assertEquals(1, interceptor.invokeCnt.get());

        assertEquals(1, interceptor.getMap.size());

        assertEquals(100, interceptor.getMap.get(key));

        checkCacheValue(key, 100);

        interceptor.reset();

        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public Object onGet(Object key, Object val) {
                return (Integer)val + 1;
            }
        };

        log.info("Get 4.");

        assertEquals((Integer)101, cache(0).get(key));

        assertEquals(1, interceptor.invokeCnt.get());

        assertEquals(1, interceptor.getMap.size());

        assertEquals(100, interceptor.getMap.get(key));

        checkCacheValue(key, 100);

        interceptor.reset();

        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public Object onGet(Object key, Object val) {
                return (Integer)val + 1;
            }
        };

        log.info("GetAsync 1.");

        assertEquals((Integer)101, cache(0).getAsync(key).get());

        assertEquals(1, interceptor.invokeCnt.get());

        assertEquals(1, interceptor.getMap.size());

        assertEquals(100, interceptor.getMap.get(key));

        checkCacheValue(key, 100);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAll() throws Exception {
        List<String> keys = new ArrayList<>();

        for (int i = 0; i < 1000; i++)
            keys.add(String.valueOf(i));

        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public Object onGet(Object key, Object val) {
                return null;
            }
        };

        Map<String, Integer> map = cache(0).getAll(keys);

        for (String key : keys)
            assertEquals(null, map.get(key));

        assertEquals(1000, interceptor.invokeCnt.get());

        interceptor.reset();

        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public Object onGet(Object key, Object val) {
                int k = Integer.valueOf((String)key);

                return k % 2 == 0 ? null : (k * 2);
            }
        };

        map = cache(0).getAll(keys);

        for (String key : keys) {
            int k = Integer.valueOf(key);

            if (k % 2 == 0)
                assertEquals(null, map.get(key));
            else
                assertEquals((Integer)(k * 2), map.get(key));
        }

        assertEquals(1000, interceptor.invokeCnt.get());

        // Put some values in cache.

        interceptor.disabled = true;

        for (int i = 0; i < 500; i++)
            cache(0).put(String.valueOf(i), i);

        interceptor.disabled = false;

        for (int j = 0; j < 2; j++) {
            interceptor.reset();

            interceptor.retInterceptor = new InterceptorAdapter() {
                @Nullable @Override public Object onGet(Object key, Object val) {
                    int k = Integer.valueOf((String)key);

                    switch (k % 3) {
                        case 0:
                            return null;

                        case 1:
                            return val;

                        case 2:
                            return k * 3;

                        default:
                            fail();
                    }

                    return null;
                }
            };

            map = j == 0 ? cache(0).getAll(keys) : cache(0).getAllAsync(keys).get();

            for (int i = 0; i < keys.size(); i++) {
                String key = keys.get(i);

                switch (i % 3) {
                    case 0:
                        assertEquals(null, map.get(key));

                        break;

                    case 1:
                        Integer exp = i < 500 ? i : null;

                        assertEquals(exp, map.get(key));

                        break;

                    case 2:
                        assertEquals((Integer)(i * 3), map.get(key));

                        break;

                    default:
                        fail();
                }
            }

            assertEquals(1000, interceptor.invokeCnt.get());
        }
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

        if (atomicityMode() == TRANSACTIONAL)
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

        if (atomicityMode() == TRANSACTIONAL)
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

        IgniteBiTuple t = interceptor.beforePutMap.get(key);

        assertEquals(null, t.get1());
        assertEquals(1, t.get2());

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

        IgniteBiTuple t = interceptor.beforePutMap.get(key);

        assertEquals(null, t.get1());
        assertEquals(1, t.get2());

        assertEquals(1, interceptor.afterPutMap.size());

        assertEquals(2, interceptor.afterPutMap.get(key));

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
            @Nullable @Override public IgniteBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new IgniteBiTuple(true, null);
            }
        };

        // Execute remove when value is null.

        log.info("Remove 1 " + op);

        remove(0, op, key, null, null);

        checkCacheValue(key, null);

        // Check values passed to interceptor.

        assertEquals(0, interceptor.beforeRemoveMap.size());

        assertEquals(null, interceptor.beforeRemoveMap.get(key));

        log.info("Remove 2 " + op);

        interceptor.reset();

        // Interceptor disables remove and changes return value.
        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public IgniteBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new IgniteBiTuple(true, 900);
            }
        };

        // Execute remove when value is null, interceptor changes return value.

        remove(0, op, key, null, 900);

        checkCacheValue(key, null);

        // Check values passed to interceptor.

        assertEquals(0, interceptor.beforeRemoveMap.size());

        assertEquals(null, interceptor.beforeRemoveMap.get(key));

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
            @Nullable @Override public IgniteBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new IgniteBiTuple(true, null);
            }
        };

        log.info("Remove 3 " + op);

        remove(0, op, key, 1, null);

        checkCacheValue(key, 1);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforeRemoveMap.size());

        assertEquals(1, interceptor.beforeRemoveMap.get(key));

        interceptor.reset();

        // Interceptor disables remove and changes return value.
        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public IgniteBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new IgniteBiTuple(true, 1000);
            }
        };

        log.info("Remove 4 " + op);

        remove(0, op, key, 1, 1000);

        checkCacheValue(key, 1);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforeRemoveMap.size());

        assertEquals(1, interceptor.beforeRemoveMap.get(key));
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
            @Nullable @Override public IgniteBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new IgniteBiTuple(false, null);
            }
        };

        // Execute remove when value is null.

        log.info("Remove 1 " + op);

        remove(0, op, key, null, null);

        checkCacheValue(key, null);

        // Check values passed to interceptor.

        assertEquals(0, interceptor.beforeRemoveMap.size());

        assertEquals(0, interceptor.afterRemoveMap.size());

        log.info("Remove 2 " + op);

        interceptor.reset();

        // Interceptor changes return value.
        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public IgniteBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new IgniteBiTuple(false, 900);
            }
        };

        // Execute remove when value is null.

        remove(0, op, key, null, 900);

        checkCacheValue(key, null);

        // Check values passed to interceptor.

        assertEquals(0, interceptor.beforeRemoveMap.size());

        assertEquals(0, interceptor.afterRemoveMap.size());

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
            @Nullable @Override public IgniteBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new IgniteBiTuple(false, null);
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
            @Nullable @Override public IgniteBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new IgniteBiTuple(false, 1000);
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
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearNodeKey() throws Exception {
        if (cacheMode() != PARTITIONED)
            return;

        if (atomicityMode() == TRANSACTIONAL) {
            for (GridCacheTxConcurrency txConcurrency : GridCacheTxConcurrency.values()) {
                for (GridCacheTxIsolation txIsolation : GridCacheTxIsolation.values()) {
                    for (Operation op : Operation.values()) {
                        // TODO: GG-8118 enable when fixed.
                        if (op == Operation.UPDATE_FILTER && txConcurrency == OPTIMISTIC)
                            continue;

                        testNearNodeKey(txConcurrency, txIsolation, op);

                        afterTest();
                    }
                }
            }
        }

        testNearNodeKey(null, null, null);
    }

    /**
     * @param txConcurrency Transaction concurrency.
     * @param txIsolation Transaction isolation.
     * @param op Operation type.
     * @throws Exception If failed.
     */
    private void testNearNodeKey(@Nullable GridCacheTxConcurrency txConcurrency,
        @Nullable GridCacheTxIsolation txIsolation, @Nullable Operation op) throws Exception {
        // Interceptor returns incremented new value.
        interceptor.retInterceptor = new InterceptorAdapter() {
            @Nullable @Override public Object onBeforePut(Object key, @Nullable Object oldVal, Object newVal) {
                return (Integer)newVal + 1;
            }
        };

        String key1 = primaryKey(0);
        String key2 = backupKey(0);
        String key3 = nearKey(0);

        interceptor.disabled = true;

        // Put from grid 1 to be sure grid 0 does not have value for near key.
        cache(1).putAll(F.asMap(key1, 1, key2, 2, key3, 3));

        interceptor.disabled = false;

        log.info("Update [op=" + op + ", key1=" + key1 + ", key2=" + key2 + ", key3=" + key3 +
            ", txConcurrency=" + txConcurrency + ", txIsolation=" + txIsolation + ']');

        if (txConcurrency != null) {
            assertNotNull(txIsolation);
            assertNotNull(op);

            try (GridCacheTx tx = cache(0).txStart(txConcurrency, txIsolation)) {
                update(0, op, key1, 100, 1);
                update(0, op, key2, 200, 2);
                update(0, op, key3, 300, 3);

                tx.commit();
            }
        }
        else
            cache(0).putAll(F.asMap(key1, 100, key2, 200, key3, 300));

        checkCacheValue(key1, 101);
        checkCacheValue(key2, 201);
        checkCacheValue(key3, 301);
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
            key1 = keys.get(0); // Need two keys for the same node to test atomic cache batch store upadte.
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
            @Nullable @Override public IgniteBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new IgniteBiTuple(false, 999);
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
            @Nullable @Override public IgniteBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new IgniteBiTuple(false, 999);
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
            @Nullable@Override public IgniteBiTuple onBeforeRemove(Object key, @Nullable Object val) {
                return new IgniteBiTuple(key.equals(key1), 999);
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
        IgniteBiTuple t = interceptor.beforePutMap.get(key);

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
     * @param idx Grid index.
     * @return Key which does not belong to the grid.
     */
    private String nearKey(int idx) {
        GridCacheAffinity aff = cache(0).affinity();

        String key = null;

        for (int i = 0; i < 10_000; i++) {
            if (!aff.isPrimaryOrBackup(grid(idx).localNode(), String.valueOf(i))) {
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
        @Nullable @Override public IgniteBiTuple onBeforeRemove(Object key, @Nullable Object val) {
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
        private final Map<Object, IgniteBiTuple> beforePutMap = new ConcurrentHashMap8<>();

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
        @SuppressWarnings("unchecked")
        @Nullable @Override public Object onGet(Object key, Object val) {
            if (disabled)
                return val;

            assertNotNull(retInterceptor);

            Object ret = retInterceptor.onGet(key, val);

            log.info("Get [key=" + key + ", val=" + val + ", ret=" + ret + ']');

            if (val != null) {
                Object old = getMap.put(key, val);

                assertNull(old); // Fot get interceptor is called on near node only.
            }

            invokeCnt.incrementAndGet();

            return ret;
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

            IgniteBiTuple t = beforePutMap.put(key, new IgniteBiTuple(oldVal, newVal));

            if (t != null) {
                assertEquals("Interceptor called with different old values for key " + key, t.get1(), oldVal);
                assertEquals("Interceptor called with different new values for key " + key, t.get2(), newVal);
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
        @Override @Nullable public IgniteBiTuple onBeforeRemove(Object key, @Nullable Object val) {
            if (disabled)
                return new IgniteBiTuple(false, val);

            assertNotNull(retInterceptor);

            IgniteBiTuple ret = retInterceptor.onBeforeRemove(key, val);

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
