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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.testframework.*;

import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;

/**
 * Base test for all multithreaded cache scenarios w/ and w/o failover.
 */
public class GridCacheRemoveAllMultithreadedTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /**
     * @return Cache mode.
     */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @return Cache atomicity mode.
     */
    @Override protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    /**
     * Actual test.
     *
     * @throws Exception If failed.
     */
    public void testRemoveAll() throws Exception {
        final Object mux = new Object();

        Thread t = new GridTestThread(new Runnable() {
            @Override public void run() {
                try {
                    while (!Thread.interrupted()) {

                        startGrid(3);

                        synchronized (mux) {
                            stopGrid(3);
                        }
                    }
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    // Test stopped.
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        t.start();

        try {
            long endTime = System.currentTimeMillis() + 60 * 1000;

            Random rnd = new Random();

            while (endTime > System.currentTimeMillis()) {
                synchronized (mux) {
                    try (IgniteDataLoader<Integer, Integer> ldr = ignite(0).dataLoader(null)) {
                        for (int i = 0; i < 1000; i++)
                            ldr.addData(i, i);
                    }
                }

                jcache(0).removeAll();

                for (int i = 0; i < gridCount(); i++) {
                    int locSize = jcache(i).localSize(CachePeekMode.ALL);

                    assert locSize == 0 : locSize;
                }
            }
        }
        finally {
            t.interrupt();

            t.join();
        }

    }
}
