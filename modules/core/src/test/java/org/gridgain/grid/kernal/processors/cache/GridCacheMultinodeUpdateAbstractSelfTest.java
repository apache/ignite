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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import javax.cache.processor.*;
import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Multinode update test.
 */
public abstract class GridCacheMultinodeUpdateAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** */
    protected static volatile boolean failed;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Nullable @Override protected GridCacheStore<?, ?> cacheStore() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 3 * 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvoke() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

        final Integer key = primaryKey(cache);

        cache.put(key, 0);

        final int THREADS = gridCount();
        final int ITERATIONS_PER_THREAD = 1000;

        Integer expVal = 0;

        for (int i = 0; i < iterations(); i++) {
            log.info("Iteration: " + i);

            final AtomicInteger gridIdx = new AtomicInteger();

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int idx = gridIdx.incrementAndGet() - 1;

                    final IgniteCache<Integer, Integer> cache = grid(idx).jcache(null);

                    for (int i = 0; i < ITERATIONS_PER_THREAD && !failed; i++)
                        cache.invoke(key, new IncProcessor());

                    return null;
                }
            }, THREADS, "invoke");

            assertFalse("Got null in processor.", failed);

            expVal += ITERATIONS_PER_THREAD * THREADS;

            for (int j = 0; j < gridCount(); j++) {
                Integer val = (Integer)grid(j).cache(null).get(key);

                assertEquals("Unexpected value for grid " + j, expVal, val);
            }
        }
    }

    /**
     * @return Number of iterations.
     */
    protected int iterations() {
        return atomicityMode() == ATOMIC ? 30 : 15;
    }

    /**
     *
     */
    protected static class IncProcessor implements EntryProcessor<Integer, Integer, Void>, Serializable {
        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, Integer> e, Object... args) {
            Integer val = e.getValue();

            if (val == null) {
                failed = true;

                System.out.println(Thread.currentThread() + " got null in processor: " + val);

                return null;
            }

            e.setValue(val + 1);

            return null;
        }
    }
}
