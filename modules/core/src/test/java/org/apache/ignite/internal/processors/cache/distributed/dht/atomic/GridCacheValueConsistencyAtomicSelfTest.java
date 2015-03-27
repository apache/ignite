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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.testframework.*;
import org.jsr166.*;

import javax.cache.processor.*;
import java.io.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.internal.processors.cache.CacheFlag.*;

/**
 * Tests cache value consistency for ATOMIC mode.
 */
public class GridCacheValueConsistencyAtomicSelfTest extends GridCacheValueConsistencyAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected int iterationCount() {
        return 100_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testForceTransformBackupConsistency() throws Exception {
        U.sleep(1000);

        int keyCnt = iterationCount() / 10;

        int threadCnt = 8;

        final int range = keyCnt / threadCnt;

        for (int r = 1; r < 5; r++) {
            final AtomicInteger rangeIdx = new AtomicInteger();

            info(">>>>>> Running iteration: " + r);

            GridTestUtils.runMultiThreaded(new Runnable() {
                @Override public void run() {
                    try {
                        int rangeStart = rangeIdx.getAndIncrement() * range;

                        info("Got range [" + rangeStart + ", " + (rangeStart + range) + ")");

                        for (int i = rangeStart; i < rangeStart + range; i++) {
                            int idx = ThreadLocalRandom8.current().nextInt(gridCount());

                            IgniteCache<Integer, Integer> cache = grid(idx).cache(null);

                            cache = ((IgniteCacheProxy<Integer, Integer>)cache).flagOn(FORCE_TRANSFORM_BACKUP);

                            cache.invoke(i, new Transformer(i));
                        }
                    }
                    catch (Exception e) {
                        throw new IgniteException(e);
                    }
                }
            }, threadCnt, "runner");

            info("Finished run, checking values.");

            U.sleep(500);

            int total = 0;

            for (int idx = 0; idx < gridCount(); idx++) {
                IgniteCache<Integer, Integer> cache = grid(idx).cache(null);

                for (int i = 0; i < keyCnt; i++) {
                    Integer val = cache.localPeek(i, CachePeekMode.ONHEAP);

                    if (val != null) {
                        assertEquals("Invalid value for key: " + i, (Integer)r, val);

                        total++;
                    }
                }
            }

            assertTrue("Total keys: " + total, total >= keyCnt * 2); // 1 backup.
        }
    }

    /**
     *
     */
    private static class Transformer implements EntryProcessor<Integer, Integer, Void>, Serializable {
        /** */
        private int key;

        /**
         * @param key Key.
         */
        private Transformer(int key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, Integer> e, Object... args) {
            Integer old = e.getValue();

            if (key < 5)
                System.err.println(Thread.currentThread().getName() + " <> Transforming value [key=" + key +
                    ", val=" + old + ']');

            e.setValue(old == null ? 1 : old + 1);

            return null;
        }
    }
}
