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

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public abstract class GridCacheValueConsistencyAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** Number of threads for test. */
    private static final int THREAD_CNT = 16;

    /** */
    private String sizePropVal;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60_000;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cCfg = super.cacheConfiguration(igniteInstanceName);

        cCfg.setCacheMode(PARTITIONED);
        cCfg.setAtomicityMode(atomicityMode());
        cCfg.setNearConfiguration(nearConfiguration());
        cCfg.setRebalanceMode(SYNC);
        cCfg.setWriteSynchronizationMode(FULL_SYNC);
        cCfg.setBackups(1);

        return cCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // Need to increase value set in GridAbstractTest
        sizePropVal = System.getProperty(IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE);

        System.setProperty(IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE, "100000");

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.setProperty(IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE, sizePropVal != null ? sizePropVal : "");
    }

    /**
     * @return Distribution mode.
     */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /**
     * @return Consistency test iteration count.
     */
    protected abstract int iterationCount();

    /**
     * @throws Exception If failed.
     */
    public void testPutRemove() throws Exception {
        awaitPartitionMapExchange();

        IgniteCache<String, Integer> cache = jcache();

        int keyCnt = 10;

        for (int i = 0; i < keyCnt; i++)
            cache.put("key" + i, i);

        for (int g = 0; g < gridCount(); g++) {
            IgniteCache<String, Integer> cache0 = jcache(g);
            ClusterNode locNode = grid(g).localNode();

            for (int i = 0; i < keyCnt; i++) {
                String key = "key" + i;

                if (ignite(0).affinity(DEFAULT_CACHE_NAME).mapKeyToPrimaryAndBackups(key).contains(locNode)) {
                    info("Node is reported as affinity node for key [key=" + key + ", nodeId=" + locNode.id() + ']');

                    assertEquals((Integer)i, cache0.localPeek(key));
                }
                else {
                    info("Node is reported as NOT affinity node for key [key=" + key +
                        ", nodeId=" + locNode.id() + ']');

                    if (nearEnabled() &&
                        ((IgniteCacheProxy)cache).context().equals(((IgniteCacheProxy)cache0).context()))
                        assertEquals((Integer)i, cache0.localPeek(key));
                    else
                        assertNull(cache0.localPeek(key));
                }

                assertEquals((Integer)i, cache0.get(key));
            }
        }

        info("Removing values from cache.");

        for (int i = 0; i < keyCnt; i++)
            assertEquals((Integer)i, cache.getAndRemove("key" + i));

        for (int g = 0; g < gridCount(); g++) {
            IgniteCache<String, Integer> cache0 = jcache(g);

            for (int i = 0; i < keyCnt; i++) {
                String key = "key" + i;

                assertNull(cache0.localPeek(key));

                assertNull(cache0.get(key));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutRemoveAll() throws Exception {
        awaitPartitionMapExchange();

        IgniteCache<String, Integer> cache = jcache();

        int keyCnt = 10;

        for (int i = 0; i < keyCnt; i++) {
            info("Putting value to cache: " + i);

            cache.put("key" + i, i);
        }

        for (int g = 0; g < gridCount(); g++) {
            IgniteCache<String, Integer> cache0 = jcache(g);
            ClusterNode locNode = grid(g).localNode();

            for (int i = 0; i < keyCnt; i++) {
                String key = "key" + i;

                if (ignite(0).affinity(DEFAULT_CACHE_NAME).mapKeyToPrimaryAndBackups(key).contains(grid(g).localNode())) {
                    info("Node is reported as affinity node for key [key=" + key + ", nodeId=" + locNode.id() + ']');

                    assertEquals((Integer)i, cache0.localPeek(key));
                }
                else {
                    info("Node is reported as NOT affinity node for key [key=" + key +
                        ", nodeId=" + locNode.id() + ']');

                    if (nearEnabled() &&
                        ((IgniteCacheProxy)cache).context().equals(((IgniteCacheProxy)cache0).context()))
                        assertEquals((Integer)i, cache0.localPeek(key));
                    else
                        assertNull(cache0.localPeek(key));
                }

                assertEquals((Integer)i, cache0.get(key));
            }
        }

        for (int g = 0; g < gridCount(); g++) {
            info(">>>> Removing all values form cache: " + g);

            jcache(g).removeAll();
        }

        info(">>>> Starting values check");

        for (int g = 0; g < gridCount(); g++) {
            IgniteCache<String, Integer> cache0 = jcache(g);

            for (int i = 0; i < keyCnt; i++) {
                String key = "key" + i;

                assertNull(cache0.localPeek(key));
                assertNull(cache0.get(key));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutConsistencyMultithreaded() throws Exception {
        if (nearEnabled())
            fail("https://issues.apache.org/jira/browse/IGNITE-627");

        for (int i = 0; i < 20; i++) {
            log.info("Iteration: " + i);

            final int range = 100;

            final int iterCnt = 100;

            final AtomicInteger threadId = new AtomicInteger();

            final AtomicInteger iters = new AtomicInteger();

            multithreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Random rnd = new Random();

                    int g = threadId.getAndIncrement();

                    Ignite ignite = grid(g);

                    IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

                    log.info("Update thread: " + ignite.name());

                    Thread.currentThread().setName("UpdateThread-" + ignite.name());

                    Long val = (long)g;

                    while (true) {
                        int i = iters.getAndIncrement();

                        if (i >= iterCnt)
                            break;

                        int k = rnd.nextInt(range);

                        cache.put(k, val);
                    }

                    return null;
                }
            }, gridCount()).get();

            checkConsistency(range);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutRemoveConsistencyMultithreaded() throws Exception {
        if (nearEnabled())
            fail("https://issues.apache.org/jira/browse/IGNITE-627");

       for (int i = 0; i < 10; i++) {
           log.info("Iteration: " + i);

           putRemoveConsistencyMultithreaded();
       }
    }

    /**
     * @throws Exception If failed.
     */
    private void putRemoveConsistencyMultithreaded() throws Exception {
        final int range = 10_000;

        final int iterCnt = iterationCount();

        final AtomicInteger iters = new AtomicInteger();

        multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Random rnd = new Random();

                while (true) {
                    int i = iters.getAndIncrement();

                    if (i >= iterCnt)
                        break;

                    int g = rnd.nextInt(gridCount());

                    Ignite ignite = grid(g);

                    IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

                    int k = rnd.nextInt(range);

                    boolean rmv = rnd.nextBoolean();

                    if (!rmv)
                        cache.put(k, Thread.currentThread().getId());
                    else
                        cache.remove(k);

                    if (i > 0 && i % 5000 == 0)
                        info("Completed: " + i);
                }

                return null;
            }
        }, THREAD_CNT).get();

        checkConsistency(range);
    }

    /**
     * @param range Key range.
     */
    private void checkConsistency(int range) {
        int present = 0;
        int absent = 0;

        Affinity<Integer> aff = ignite(0).affinity(DEFAULT_CACHE_NAME);

        boolean invalidVal = false;

        for (int i = 0; i < range; i++) {
            Long firstVal = null;

            for (int g = 0; g < gridCount(); g++) {
                Ignite ignite = grid(g);

                Long val = (Long)ignite.cache(DEFAULT_CACHE_NAME).localPeek(i);

                if (firstVal == null && val != null)
                    firstVal = val;

                if (val != null) {
                    if (!firstVal.equals(val)) {
                        invalidVal = true;

                        boolean primary = aff.isPrimary(ignite.cluster().localNode(), i);
                        boolean backup = aff.isBackup(ignite.cluster().localNode(), i);

                        log.error("Invalid value detected [key=" + i +
                            ", val=" + val +
                            ", firstVal=" + firstVal +
                            ", node=" + g +
                            ", primary=" + primary +
                            ", backup=" + backup + ']');

                        log.error("All values: ");

                        printValues(aff, i);

                        break;
                    }
                }
            }

            if (firstVal == null)
                absent++;
            else
                present++;
        }

        assertFalse("Inconsistent value found.", invalidVal);

        info("Finished check [present=" + present + ", absent=" + absent + ']');

        info("Checking keySet consistency");

    }

    /**
     * @param aff Affinity.
     * @param key Key.
     */
    private void printValues(Affinity<Integer> aff, int key) {
        for (int g = 0; g < gridCount(); g++) {
            Ignite ignite = grid(g);

            boolean primary = aff.isPrimary(ignite.cluster().localNode(), key);
            boolean backup = aff.isBackup(ignite.cluster().localNode(), key);

            Object val = ignite.cache(DEFAULT_CACHE_NAME).localPeek(key);

            log.error("Node value [key=" + key +
                ", val=" + val +
                ", node=" + g +
                ", primary=" + primary +
                ", backup=" + backup + ']');
        }
    }
}