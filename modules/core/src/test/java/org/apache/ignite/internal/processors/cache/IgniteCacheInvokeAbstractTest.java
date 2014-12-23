/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.gridgain.grid.cache.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import javax.cache.processor.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 *
 */
public abstract class IgniteCacheInvokeAbstractTest extends IgniteCacheAbstractTest {
    /** */
    private Integer lastKey = 0;

    /**
     * @throws Exception If failed.
     */
    public void testInvoke() throws Exception {
        // TODO IGNITE41 test with forceTransformBackups.

        final IgniteCache<Integer, Integer> cache = jcache();

        IncrementProcessor incProcessor = new IncrementProcessor();

        for (final Integer key : keys()) {
            log.info("Test invoke [key=" + key + ']');

            cache.remove(key);

            Integer res = cache.invoke(key, incProcessor);

            assertEquals(-1, (int)res);

            checkValue(key, 1);

            res = cache.invoke(key, incProcessor);

            assertEquals(1, (int)res);

            checkValue(key, 2);

            res = cache.invoke(key, incProcessor);

            assertEquals(2, (int)res);

            checkValue(key, 3);

            res = cache.invoke(key, new ArgumentsSumProcessor(), 10, 20, 30);

            assertEquals(3, (int)res);

            checkValue(key, 63);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    cache.invoke(key, new ExceptionProcessor(63));

                    return null;
                }
            }, EntryProcessorException.class, "Test processor exception.");

            checkValue(key, 63);

            assertNull(cache.invoke(key, new RemoveProcessor(63)));

            checkValue(key, null);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAll() throws Exception {
        IgniteCache<Integer, Integer> cache = jcache();

        invokeAll(cache, new HashSet<>(primaryKeys(cache, 3, 0)));

        invokeAll(cache, new HashSet<>(backupKeys(cache, 3, 0)));

        invokeAll(cache, new HashSet<>(nearKeys(cache, 3, 0)));

        Set<Integer> keys = new HashSet<>();

        keys.addAll(primaryKeys(jcache(0), 3, 0));
        keys.addAll(primaryKeys(jcache(1), 3, 0));
        keys.addAll(primaryKeys(jcache(2), 3, 0));

        invokeAll(cache, keys);

        keys = new HashSet<>();

        for (int i = 0; i < 1000; i++)
            keys.add(i);

        invokeAll(cache, keys);
    }

    /**
     * @param cache Cache.
     * @param keys Keys.
     */
    private void invokeAll(IgniteCache<Integer, Integer> cache, Set<Integer> keys) {
        cache.removeAll(keys);

        log.info("Test invokeAll [keys=" + keys + ']');

        IncrementProcessor incProcessor = new IncrementProcessor();

        Map<Integer, EntryProcessorResult<Integer>> resMap = cache.invokeAll(keys, incProcessor);

        Map<Object, Object> exp = new HashMap<>();

        for (Integer key : keys)
            exp.put(key, -1);

        checkResult(resMap, exp);

        for (Integer key : keys)
            checkValue(key, 1);

        resMap = cache.invokeAll(keys, incProcessor);

        exp = new HashMap<>();

        for (Integer key : keys)
            exp.put(key, 1);

        checkResult(resMap, exp);

        for (Integer key : keys)
            checkValue(key, 2);

        resMap = cache.invokeAll(keys, new ArgumentsSumProcessor(), 10, 20, 30);

        for (Integer key : keys)
            exp.put(key, 3);

        checkResult(resMap, exp);

        for (Integer key : keys)
            checkValue(key, 62);

        resMap = cache.invokeAll(keys, new ExceptionProcessor(null));

        for (Integer key : keys) {
            final EntryProcessorResult<Integer> res = resMap.get(key);

            assertNotNull("No result for " + key);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    res.get();

                    return null;
                }
            }, EntryProcessorException.class, "Test processor exception.");
        }

        for (Integer key : keys)
            checkValue(key, 62);

        resMap = cache.invokeAll(keys, new RemoveProcessor(null));

        for (Integer key : keys) {
            final EntryProcessorResult<Integer> res = resMap.get(key);

            assertNotNull("No result for " + key);

            assertNull(res.get());
        }

        for (Integer key : keys)
            checkValue(key, null);
    }

    /**
     * @param resMap Result map.
     * @param exp Expected results.
     */
    private void checkResult(Map<Integer, EntryProcessorResult<Integer>> resMap, Map<Object, Object> exp) {
        assertNotNull(resMap);

        assertEquals(exp.size(), resMap.size());

        for (Map.Entry<Object, Object> expVal : exp.entrySet()) {
            EntryProcessorResult<Integer> res = resMap.get(expVal.getKey());

            assertNotNull("No result for " + expVal.getKey());

            assertEquals("Unexpected result for " + expVal.getKey(), res.get(), expVal.getValue());
        }
    }

    /**
     * @param key Key.
     * @param expVal Expected value.
     */
    protected void checkValue(Object key, @Nullable Object expVal) {
        if (expVal != null) {
            for (int i = 0; i < gridCount(); i++) {
                IgniteCache<Object, Object> cache = jcache(i);

                Object val = cache.localPeek(key);

                if (val == null)
                    assertFalse(cache(0).affinity().isPrimaryOrBackup(ignite(i).cluster().localNode(), key));
                else
                    assertEquals("Unexpected value for grid " + i, expVal, val);
            }
        }
        else {
            for (int i = 0; i < gridCount(); i++) {
                IgniteCache<Object, Object> cache = jcache(i);

                assertNull("Unexpected non null value for grid " + i, cache.localPeek(key));
            }
        }
    }

    /**
     * @return Test keys.
     * @throws Exception If failed.
     */
    private Collection<Integer> keys() throws Exception {
        GridCache<Integer, Object> cache = cache(0);

        ArrayList<Integer> keys = new ArrayList<>();

        keys.add(primaryKeys(cache, 1, lastKey).get(0));

        if (gridCount() > 1) {
            keys.add(backupKeys(cache, 1, lastKey).get(0));

            if (cache.configuration().getCacheMode() != REPLICATED)
                keys.add(nearKeys(cache, 1, lastKey).get(0));
        }

        lastKey = Collections.max(keys) + 1;

        return keys;
    }

    /**
     *
     */
    private static class ArgumentsSumProcessor implements EntryProcessor<Integer, Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> e, Object... args)
            throws EntryProcessorException {
            assertEquals(3, args.length);
            assertEquals(10, args[0]);
            assertEquals(20, args[1]);
            assertEquals(30, args[2]);

            assertTrue(e.exists());

            Integer res = e.getValue();

            for (Object arg : args)
                res += (Integer)arg;

            e.setValue(res);

            return args.length;
        }
    }

    /**
     *
     */
    private static class IncrementProcessor implements EntryProcessor<Integer, Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> e,
            Object... arguments) throws EntryProcessorException {
            if (e.exists()) {
                Integer val = e.getValue();

                assertNotNull(val);

                e.setValue(val + 1);

                assertTrue(e.exists());

                assertEquals(val + 1, (int) e.getValue());

                return val;
            }
            else {
                e.setValue(1);

                return -1;
            }
        }
    }

    /**
     *
     */
    private static class RemoveProcessor implements EntryProcessor<Integer, Integer, Integer> {
        /** */
        private Integer expVal;

        /**
         * @param expVal Expected value.
         */
        RemoveProcessor(@Nullable Integer expVal) {
            this.expVal = expVal;
        }

        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> e,
            Object... arguments) throws EntryProcessorException {
            assertTrue(e.exists());

            if (expVal != null)
                assertEquals(expVal, e.getValue());

            e.remove();

            assertFalse(e.exists());

            return null;
        }
    }

    /**
     *
     */
    private static class ExceptionProcessor implements EntryProcessor<Integer, Integer, Integer> {
        /** */
        private Integer expVal;

        /**
         * @param expVal Expected value.
         */
        ExceptionProcessor(@Nullable Integer expVal) {
            this.expVal = expVal;
        }

        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> e,
            Object... arguments) throws EntryProcessorException {
            assertTrue(e.exists());

            if (expVal != null)
                assertEquals(expVal, e.getValue());

            throw new EntryProcessorException("Test processor exception.");
        }
    }
}
