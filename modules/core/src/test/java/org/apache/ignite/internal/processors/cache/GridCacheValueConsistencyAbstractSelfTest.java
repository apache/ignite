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
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

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
        return 60000;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cCfg = super.cacheConfiguration(gridName);

        cCfg.setCacheMode(PARTITIONED);
        cCfg.setAtomicityMode(atomicityMode());
        cCfg.setAtomicWriteOrderMode(writeOrderMode());
        cCfg.setDistributionMode(distributionMode());
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
    @Override protected CacheDistributionMode distributionMode() {
        return PARTITIONED_ONLY;
    }

    /**
     * @return Atomic write order mode.
     */
    protected CacheAtomicWriteOrderMode writeOrderMode() {
        return CLOCK;
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

                if (ignite(0).affinity(null).mapKeyToPrimaryAndBackups(key).contains(locNode)) {
                    info("Node is reported as affinity node for key [key=" + key + ", nodeId=" + locNode.id() + ']');

                    assertEquals((Integer)i, cache0.localPeek(key, CachePeekMode.ONHEAP));
                }
                else {
                    info("Node is reported as NOT affinity node for key [key=" + key +
                        ", nodeId=" + locNode.id() + ']');

                    if (distributionMode() == NEAR_PARTITIONED && cache == cache0)
                        assertEquals((Integer)i, cache0.localPeek(key, CachePeekMode.ONHEAP));
                    else
                        assertNull(cache0.localPeek(key, CachePeekMode.ONHEAP));
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

                assertNull(cache0.localPeek(key, CachePeekMode.ONHEAP));

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

                if (ignite(0).affinity(null).mapKeyToPrimaryAndBackups(key).contains(grid(g).localNode())) {
                    info("Node is reported as affinity node for key [key=" + key + ", nodeId=" + locNode.id() + ']');

                    assertEquals((Integer)i, cache0.localPeek(key, CachePeekMode.ONHEAP));
                }
                else {
                    info("Node is reported as NOT affinity node for key [key=" + key +
                        ", nodeId=" + locNode.id() + ']');

                    if (distributionMode() == NEAR_PARTITIONED && cache == cache0)
                        assertEquals((Integer)i, cache0.localPeek(key, CachePeekMode.ONHEAP));
                    else
                        assertNull(cache0.localPeek(key, CachePeekMode.ONHEAP));
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

                assertNull(cache0.localPeek(key, CachePeekMode.ONHEAP));
                assertNull(cache0.get(key));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutRemoveConsistencyMultithreaded() throws Exception {
        final int range = 10000;

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

                    IgniteCache<Object, Object> cache = ignite.jcache(null);

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

        int present = 0;
        int absent = 0;

        for (int i = 0; i < range; i++) {
            Long firstVal = null;

            for (int g = 0; g < gridCount(); g++) {
                Long val = (Long)grid(g).jcache(null).localPeek(i, CachePeekMode.ONHEAP);

                if (firstVal == null && val != null)
                    firstVal = val;

                assert val == null || firstVal.equals(val) : "Invalid value detected [val=" + val +
                    ", firstVal=" + firstVal + ']';
            }

            if (firstVal == null)
                absent++;
            else
                present++;
        }

        info("Finished check [present=" + present + ", absent=" + absent + ']');

        info("Checking keySet consistency");

        for (int g = 0; g < gridCount(); g++)
            checkKeySet(grid(g));
    }

    /**
     * @param g Grid to check.
     */
    private void checkKeySet(Ignite g) {
        GridCache<Object, Object> cache = ((IgniteKernal)g).internalCache(null);

        Set<Object> keys = cache.keySet();

        int cacheSize = cache.size();
        int keySetSize = keys.size();

        int itSize = 0;

        for (Object ignored : keys)
            itSize++;

        int valsSize = cache.values().size();

        info("cacheSize=" + cacheSize + ", keysSize=" + keySetSize + ", valsSize=" + valsSize +
            ", itSize=" + itSize + ']');

        assertEquals("cacheSize vs itSize", cacheSize, itSize);
        assertEquals("cacheSize vs keySeySize", cacheSize, keySetSize);
    }
}
