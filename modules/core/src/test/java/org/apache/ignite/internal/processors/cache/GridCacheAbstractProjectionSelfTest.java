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
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.internal.util.typedef.*;

import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.cache.GridCacheAtomicityMode.*;
import static org.apache.ignite.cache.GridCacheFlag.*;
import static org.apache.ignite.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Tests for custom cache projection (with filters and flags).
 */
public abstract class GridCacheAbstractProjectionSelfTest extends GridCacheAbstractSelfTest {
    /** Test timeout */
    private static final long TEST_TIMEOUT = 120 * 1000;

    /** Number of grids to start. */
    private static final int GRID_CNT = 1;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setCacheMode(cacheMode());
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setPreloadMode(GridCachePreloadMode.SYNC);

        return cfg;
    }

    /**
     * @return Cache mode.
     */
    @Override protected abstract GridCacheMode cacheMode();

    /**
     * @return Cache instance.
     */
    @SuppressWarnings({"TypeMayBeWeakened"})
    private GridCache<String, TestCloneable> cacheCloneable() {
        return grid(0).cache(null);
    }

    /**
     * Test cloneable.
     */
    private static class TestCloneable implements Cloneable {
        /** */
        private String str;

        /**
         * Default constructor.
         */
        private TestCloneable() {
            // No-op.
        }

        /**
         * @param str String value.
         */
        private TestCloneable(String str) {
            this.str = str;
        }

        /**
         * @return str value.
         */
        private String str() {
            return str;
        }

        /** {@inheritDoc} */
        @Override public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }

    /** */
    private IgniteBiPredicate<String, Integer> kvFilter = new P2<String, Integer>() {
        @Override public boolean apply(String key, Integer val) {
            return key.contains("key") && val >= 0;
        }
    };

    /** */
    private IgnitePredicate<GridCacheEntry<String, Integer>> entryFilter = new P1<GridCacheEntry<String, Integer>>() {
        @Override public boolean apply(GridCacheEntry<String, Integer> e) {
            Integer val = e.peek();

            // Let's assume that null values will be passed through, otherwise we won't be able
            // to put any new values to cache using projection with this entry filter.
            return e.getKey().contains("key") && (val == null || val >= 0);
        }
    };

    /**
     * Asserts that given runnable throws specified exception.
     *
     * @param exCls Expected exception.
     * @param r Runnable to check.
     * @throws Exception If check failed.
     */
    private void assertException(Class<? extends Exception> exCls, Runnable r) throws Exception {
        assert exCls != null;
        assert r != null;

        try {
            r.run();

            fail(exCls.getSimpleName() + " must have been thrown.");
        }
        catch (Exception e) {
            if (e.getClass() != exCls)
                throw e;

            info("Caught expected exception: " + e);
        }
    }

    /**
     * @param r Runnable.
     * @throws Exception If check failed.
     */
    private void assertFlagException(Runnable r) throws Exception {
        assertException(GridCacheFlagException.class, r);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testTypeProjection() throws Exception {
        GridCache<String, Integer> cache = cache();

        cache.putAll(F.asMap("k1", 1 , "k2", 2, "k3", 3));

        GridCache<Double, Boolean> anotherCache = grid(0).cache(null);

        assert anotherCache != null;

        anotherCache.put(3.14, true);

        GridCacheProjection<String, Integer> prj = cache.projection(String.class, Integer.class);

        List<String> keys = F.asList("k1", "k2", "k3");

        for (String key : keys)
            assert prj.containsKey(key);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testSize() throws Exception {
        GridCacheProjection<String, Integer> prj = cache().projection(kvFilter);

        assert prj.cache() != null;

        int size = 10;

        if (atomicityMode() == TRANSACTIONAL) {
            IgniteTx tx = prj.txStart();

            for (int i = 0; i < size; i++)
                prj.put("key" + i, i);

            prj.put("k", 11);
            prj.put("key", -1);

            tx.commit();
        }
        else {
            for (int i = 0; i < size; i++)
                prj.put("key" + i, i);

            prj.put("k", 11);
            prj.put("key", -1);
        }

        assertEquals(size, cache().size());
        assertEquals(size, prj.size());
    }

    /**
     * @throws Exception In case of error.
     */
    public void testContainsKey() throws Exception {
        cache().put("key", 1);
        cache().put("k", 2);

        assert cache().containsKey("key");
        assert cache().containsKey("k");
        assert !cache().containsKey("wrongKey");

        GridCacheProjection<String, Integer> prj = cache().projection(kvFilter);

        assert prj.containsKey("key");
        assert !prj.containsKey("k");
        assert !prj.containsKey("wrongKey");

        assert prj.projection(F.<GridCacheEntry<String, Integer>>alwaysTrue()).containsKey("key");
        assert !prj.projection(F.<GridCacheEntry<String, Integer>>alwaysFalse()).containsKey("key");
        assert !prj.projection(F.<GridCacheEntry<String, Integer>>alwaysFalse()).containsKey("k");
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPut() throws Exception {
        final GridCacheProjection<String, Integer> prj = cache().projection(kvFilter);

        prj.put("key", 1);
        prj.put("k", 2);

        assert prj.containsKey("key");
        assert !prj.containsKey("k");

        assertFlagException(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                prj.flagsOn(LOCAL).put("key", 1);
            }
        });

        assertFlagException(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                prj.flagsOn(READ).put("key", 1);
            }
        });
    }

    /**
     * @throws Exception In case of error.
     */
    public void testLocalFlag() throws Exception {
        GridCacheProjection<String, Integer> prj = cache().projection(entryFilter);

        final GridCacheProjection<String, Integer> locPrj = prj.flagsOn(LOCAL);

        prj.put("key", 1);

        Integer one = 1;

        assertEquals(one, prj.get("key"));

        assertFlagException(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                locPrj.put("key", 1);
            }
        });

        prj.get("key");

        assertFlagException(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                locPrj.get("key");
            }
        });

        prj.getAll(F.asList("key", "key1"));

        assertFlagException(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                locPrj.getAll(F.asList("key", "key1"));
            }
        });

        prj.remove("key");

        assertFlagException(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                locPrj.remove("key");
            }
        });

        prj.put("key", 1);

        assertEquals(one, prj.replace("key", 2));

        assertFlagException(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                locPrj.replace("key", 3);
            }
        });

        prj.removeAll(F.asList("key"));

        assert !prj.containsKey("key");

        prj.put("key", 1);

        assertFlagException(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                locPrj.removeAll(F.asList("key"));
            }
        });

        assert prj.containsKey("key");

        assert locPrj.containsKey("key");

        assertFlagException(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                locPrj.reload("key");
            }
        });

        assertEquals(one, locPrj.peek("key"));

        locPrj.evict("key");

        assert !locPrj.containsKey("key");

        locPrj.promote("key");

        assert locPrj.containsKey("key");

        locPrj.clear("key");

        assert !locPrj.containsKey("key");
    }

    /**
     * @throws Exception In case of error.
     */
    public void testEntryLocalFlag() throws Exception {
        GridCacheProjection<String, Integer> prj = cache().projection(entryFilter);

        GridCacheProjection<String, Integer> loc = prj.flagsOn(LOCAL);

        prj.put("key", 1);

        GridCacheEntry<String, Integer> prjEntry = prj.entry("key");
        final GridCacheEntry<String, Integer> locEntry = loc.entry("key");

        assert prjEntry != null;
        assert locEntry != null;

        Integer one = 1;

        assertEquals(one, prjEntry.getValue());

        assertFlagException(new CA() {
            @Override public void apply() {
                locEntry.setValue(1);
            }
        });

        assertEquals(one, prjEntry.getValue());

        assertFlagException(new CA() {
            @Override public void apply() {
                locEntry.getValue();
            }
        });

        prjEntry.remove();

        assertFlagException(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                locEntry.remove();
            }
        });

        prjEntry.set(1);

        assertEquals(one, prjEntry.replace(2));

        assertFlagException(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                locEntry.replace(3);
            }
        });

        assertFlagException(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                locEntry.reload();
            }
        });

        prj.put("key", 1);

        assertEquals(one, locEntry.peek());

        locEntry.evict();

        assert locEntry.peek() == null;

        loc.promote("key");

        assert loc.containsKey("key");

        locEntry.clear();

        assert locEntry.peek() == null;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testReadFlag() throws Exception {
        GridCacheProjection<String, Integer> prj = cache().projection(entryFilter);

        final GridCacheProjection<String, Integer> readPrj = prj.flagsOn(READ);

        prj.put("key", 1);

        Integer one = 1;

        assertEquals(one, prj.get("key"));

        assertFlagException(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                readPrj.put("key", 1);
            }
        });

        prj.remove("key");

        assertFlagException(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                readPrj.remove("key");
            }
        });

        prj.put("key", 1);

        assertEquals(one, prj.replace("key", 2));

        assertFlagException(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                readPrj.replace("key", 3);
            }
        });

        prj.removeAll(F.asList("key"));

        assert !prj.containsKey("key");

        prj.put("key", 1);

        assertFlagException(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                readPrj.removeAll(F.asList("key"));
            }
        });

        assertFlagException(new CA() {
            @Override public void apply() {
                readPrj.evict("key");
            }
        });

        assert prj.containsKey("key");

        assertFlagException(new CA() {
            @Override public void apply() {
                readPrj.clear("key");
            }
        });

        assert prj.containsKey("key");

        assertFlagException(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                readPrj.reload("key");
            }
        });

        assert prj.containsKey("key");

        assertFlagException(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                readPrj.promote("key");
            }
        });

        assert prj.containsKey("key");

        readPrj.get("key");

        readPrj.getAll(F.asList("key", "key1"));

        assertEquals(one, readPrj.peek("key"));
    }

    /**
     * @param clone Cloned value.
     * @param original Original value.
     */
    private void checkClone(TestCloneable clone, TestCloneable original) {
        assert original != null;
        assert clone != null;
        assert clone != original;
        assertEquals(clone.str(), original.str());
    }

    /**
     * @throws Exception In case of error.
     */
    @SuppressWarnings({"UnnecessaryFinalOnLocalVariable"})
    public void testCloneFlag() throws Exception {
        GridCacheProjection<String, TestCloneable> prj = cacheCloneable().flagsOn(CLONE);

        final TestCloneable val = new TestCloneable("val");

        prj.put("key", val);

        checkClone(prj.get("key"), val);

        checkClone(prj.getAsync("key").get(), val);

        Map<String, TestCloneable> map = prj.getAll(F.asList("key"));

        assertEquals(1, map.size());

        checkClone(map.get("key"), val);

        map = prj.getAllAsync(F.asList("key")).get();

        assertEquals(1, map.size());

        checkClone(map.get("key"), val);

        checkClone(prj.peek("key"), val);

        Collection<TestCloneable> vals = prj.values();

        assert vals != null;
        assertEquals(1, vals.size());

        checkClone(vals.iterator().next(), val);

        Set<GridCacheEntry<String, TestCloneable>> entries = prj.entrySet();

        assertEquals(1, entries.size());

        checkClone(entries.iterator().next().getValue(), val);

        GridCacheEntry<String, TestCloneable> entry = prj.entry("key");

        assert entry != null;

        checkClone(entry.peek(), val);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testEntryParent() throws Exception {
        cache().put("key", 1);

        GridCacheProxyImpl<String, Integer> prj = (GridCacheProxyImpl<String, Integer>)cache().
            flagsOn(CLONE, INVALIDATE);

        GridCacheEntry<String, Integer> entry = prj.entry("key");

        assert entry != null;

        GridCacheProxyImpl<String, Integer> entryPrj = (GridCacheProxyImpl<String, Integer>)entry.projection();

        assert entryPrj.delegate() == prj.delegate();
    }

    /**
     * @throws Exception if failed.
     */
    public void testSkipStoreFlag() throws Exception {
        assertNull(cache().put("kk1", 100500));
        assertEquals(100500, map.get("kk1"));

        IgniteCache<String, Integer> c = jcache().withSkipStore();

        assertNull(c.getAndPut("noStore", 123));
        assertEquals(123, (Object) c.get("noStore"));
        assertNull(map.get("noStore"));

        assertTrue(c.remove("kk1", 100500));
        assertEquals(100500, map.get("kk1"));
        assertNull(c.get("kk1"));
        assertEquals(100500, (Object) cache().get("kk1"));
    }

    /**
     * @throws Exception if failed.
     */
    // TODO: enable when GG-7579 is fixed.
    public void _testSkipStoreFlagMultinode() throws Exception {
        final int nGrids = 3;

        // Start additional grids.
        for (int i = 1; i < nGrids; i++)
            startGrid(i);

        try {
            testSkipStoreFlag();
        }
        finally {
            for (int i = 1; i < nGrids; i++)
                stopGrid(i);
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testSkipSwapFlag() throws Exception {
        cache().put("key", 1);

        cache().evict("key");

        assert cache().peek("key") == null;

        Integer one = 1;

        assertEquals(one, cache().get("key"));

        cache().evict("key");

        assertEquals(one, cache().reload("key"));

        cache().remove("key");

        assertFalse(cache().containsKey("key"));
        assertNull(cache().get("key"));

        GridCacheProjection<String, Integer> prj = cache().flagsOn(SKIP_SWAP, SKIP_STORE);

        prj.put("key", 1);

        assertEquals(one, prj.get("key"));
        assertEquals(one, prj.peek("key"));

        assert prj.evict("key");

        assert prj.peek("key") == null;
        assert prj.get("key") == null;
    }

    /**
     * Checks that previous entry in update operations is taken
     * from swap after eviction, even if SKIP_SWAP is enabled.
     *
     * @throws Exception If error happens.
     */
    public void testSkipSwapFlag2() throws Exception {
        cache().put("key", 1);

        cache().evict("key");

        GridCacheProjection<String, Integer> prj = cache().flagsOn(SKIP_SWAP, SKIP_STORE);

        assertNull(prj.get("key"));

        Integer old = prj.put("key", 2);

        assertEquals(Integer.valueOf(1), old); // Update operations on cache should not take into account SKIP_SWAP flag.

        prj.remove("key");
    }

    /**
     * Tests {@link GridCacheFlag#SKIP_SWAP} flag on multiple nodes.
     *
     * @throws Exception If error occurs.
     */
    public void testSkipSwapFlagMultinode() throws Exception {
        final int nGrids = 3;

        // Start additional grids.
        for (int i = 1; i < nGrids; i++)
            startGrid(i);

        try {
            final int nEntries = 100;

            // Put the values in cache.
            for (int i = 1; i <= nEntries; i++)
                grid(0).cache(null).put(i, i);

            // Evict values from cache. Values should go to swap.
            for (int i = 0; i < nGrids; i++) {
                grid(i).cache(null).evictAll();

                assertTrue("Grid #" + i + " has empty swap.", grid(i).cache(null).swapIterator().hasNext());
            }

            // Set SKIP_SWAP flag.
            GridCacheProjection<Object, Object> cachePrj = grid(0).cache(null).flagsOn(SKIP_SWAP, SKIP_STORE);

            // Put new values.
            for (int i = 1; i <= nEntries; i++)
                assertEquals(i, cachePrj.put(i, i + 1)); // We should get previous values from swap, disregarding SKIP_SWAP.

            // Swap should be empty now.
            for (int i = 0; i < nGrids; i++)
                assertFalse("Grid #" + i + " has non-empty swap.", grid(i).cache(null).swapIterator().hasNext());
        }
        finally {
            // Stop started grids.
            for (int i = 1; i < nGrids; i++)
                stopGrid(i);
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testTx() throws Exception {
        if (atomicityMode() == ATOMIC)
            return;

        IgniteTx tx = cache().txStart();

        GridCacheProjection<String, Integer> typePrj = cache().projection(String.class, Integer.class);

        typePrj.put("key", 1);
        typePrj.put("k", 2);

        GridCacheProjection<String, Integer> kvFilterPrj = cache().projection(kvFilter);

        Integer one = 1;

        assertEquals(one, kvFilterPrj.get("key"));
        assert kvFilterPrj.get("k") == null;

        GridCacheProjection<String, Integer> entryFilterPrj = cache().projection(entryFilter);

        assertEquals(one, entryFilterPrj.get("key"));
        assert entryFilterPrj.get("k") == null;

        // Now will check projection on projection.
        kvFilterPrj = typePrj.projection(kvFilter);

        assertEquals(one, kvFilterPrj.get("key"));
        assert kvFilterPrj.get("k") == null;

        entryFilterPrj = typePrj.projection(entryFilter);

        assertEquals(one, entryFilterPrj.get("key"));
        assert entryFilterPrj.get("k") == null;

        typePrj = cache().projection(entryFilter).projection(String.class, Integer.class);

        assertEquals(one, typePrj.get("key"));
        assertNull(typePrj.get("k"));

        tx.commit();

        TransactionsConfiguration tCfg = grid(0).configuration().getTransactionsConfiguration();

        tx = cache().txStart(
            tCfg.getDefaultTxConcurrency(),
            tCfg.getDefaultTxIsolation(),
            tCfg.getDefaultTxTimeout(),
            0
        );

        // Try to change tx property.
        assertFlagException(new CA() {
            @Override public void apply() {
                cache().flagsOn(INVALIDATE);
            }
        });

        assertFlagException(new CA() {
            @Override public void apply() {
                cache().projection(entryFilter).flagsOn(INVALIDATE);
            }
        });

        tx.commit();
    }

    /**
     * @throws IgniteCheckedException In case of error.
     */
    public void testTypedProjection() throws Exception {
        GridCache<Object, Object> cache = grid(0).cache(null);

        cache.putx("1", "test string");
        cache.putx("2", 0);

        final GridCacheProjection<String, String> prj = cache.projection(String.class, String.class);

        final CountDownLatch latch = new CountDownLatch(1);

        prj.removeAll(new P1<GridCacheEntry<String, String>>() {
            @Override
            public boolean apply(GridCacheEntry<String, String> e) {
                info(" --> " + e.peek().getClass());

                latch.countDown();

                return true;
            }
        });

        assertTrue(latch.await(1, SECONDS));
    }
}
