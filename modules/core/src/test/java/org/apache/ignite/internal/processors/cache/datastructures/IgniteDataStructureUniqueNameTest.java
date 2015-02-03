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

package org.apache.ignite.internal.processors.cache.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;

/**
 *
 */
public class IgniteDataStructureUniqueNameTest extends IgniteCollectionAbstractTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode collectionCacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode collectionCacheAtomicityMode() {
        return ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        IgniteAtomicConfiguration atomicCfg = new IgniteAtomicConfiguration();

        atomicCfg.setBackups(1);
        atomicCfg.setCacheMode(PARTITIONED);

        cfg.setAtomicConfiguration(atomicCfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testUniqueNameMultithreaded() throws Exception {
        testUniqueName(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testUniqueNameMultinode() throws Exception {
        testUniqueName(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateRemove() throws Exception {
        final String name = IgniteUuid.randomUuid().toString();

        final Ignite ignite = ignite(0);

        assertNull(ignite.atomicLong(name, 0, false));

        IgniteAtomicReference<Integer> ref = ignite.atomicReference(name, 0, true);

        assertNotNull(ref);

        assertSame(ref, ignite.atomicReference(name, 0, true));

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite.atomicLong(name, 0, false);

                return null;
            }
        }, IgniteException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite.atomicLong(name, 0, true);

                return null;
            }
        }, IgniteException.class, null);

        ref.close();

        IgniteAtomicLong atomicLong = ignite.atomicLong(name, 0, true);

        assertNotNull(atomicLong);

        assertSame(atomicLong, ignite.atomicLong(name, 0, true));

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite.atomicReference(name, 0, false);

                return null;
            }
        }, IgniteException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite.queue(name, config(false), 0, true);

                return null;
            }
        }, IgniteException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite.queue(name, null, 0, false);

                return null;
            }
        }, IgniteException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite.set(name, config(false), true);

                return null;
            }
        }, IgniteException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite.set(name, null, false);

                return null;
            }
        }, IgniteException.class, null);

        atomicLong.close();

        IgniteQueue<Integer> q = ignite.queue(name, config(false), 0, true);

        assertNotNull(q);

        assertSame(q, ignite.queue(name, config(false), 0, true));

        assertSame(q, ignite.queue(name, null, 0, false));

        q.close();

        assertNull(ignite.set(name, null, false));

        IgniteSet<Integer> set = ignite.set(name, config(false), true);

        assertNotNull(set);

        assertSame(set, ignite.set(name, config(false), true));

        assertSame(set, ignite.set(name, null, false));
        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite.atomicReference(name, 0, false);

                return null;
            }
        }, IgniteException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite.queue(name, config(false), 0, true);

                return null;
            }
        }, IgniteException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ignite.queue(name, null, 0, false);

                return null;
            }
        }, IgniteException.class, null);

        set.close();

        ref = ignite.atomicReference(name, 0, true);

        assertNotNull(ref);

        assertSame(ref, ignite.atomicReference(name, 0, true));
    }

    /**
     * @param singleGrid If {@code true} uses single grid.
     * @throws Exception If failed.
     */
    private void testUniqueName(final boolean singleGrid) throws Exception {
        final String name = IgniteUuid.randomUuid().toString();

        final int THREADS = 10;

        for (int iter = 0; iter < 10; iter++) {
            log.info("Iteration: " + iter);

            List<IgniteInternalFuture<Object>> futs = new ArrayList<>(THREADS);

            for (int i = 0; i < THREADS; i++) {
                final int idx = i;

                IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try {
                            Ignite ignite = singleGrid ? ignite(0) : ignite(idx % gridCount());

                            switch (idx % 5) {
                                case 0:
                                    log.info("Create atomic long, grid: " + ignite.name());

                                    return ignite.atomicLong(name, 0, true);

                                case 1:
                                    log.info("Create atomic sequence, grid: " + ignite.name());

                                    return ignite.atomicSequence(name, 0, true);

                                case 2:
                                    log.info("Create atomic stamped, grid: " + ignite.name());

                                    return ignite.atomicStamped(name, 0, true, true);

                                case 3:
                                    log.info("Create atomic latch, grid: " + ignite.name());

                                    return ignite.countDownLatch(name, 0, true, true);

                                case 4:
                                    log.info("Create atomic reference, grid: " + ignite.name());

                                    return ignite.atomicReference(name, null, true);

                                default:
                                    fail();

                                    return null;
                            }
                        }
                        catch (IgniteException e) {
                            log.info("Failed: " + e);

                            return e;
                        }
                    }
                });

                futs.add(fut);
            }

            Closeable dataStructure = null;

            for (IgniteInternalFuture<Object> fut : futs) {
                Object res = fut.get();

                if (res instanceof IgniteException)
                    continue;

                assertTrue("Unexpected object: " + res,
                    res instanceof IgniteAtomicLong ||
                        res instanceof IgniteAtomicSequence ||
                        res instanceof IgniteAtomicReference ||
                        res instanceof IgniteAtomicStamped ||
                        res instanceof IgniteCountDownLatch);

                if (dataStructure != null) {
                    log.info("Data structure created: " + dataStructure);

                    assertEquals(dataStructure.getClass(), res.getClass());
                }
                else
                    dataStructure = (Closeable)res;
            }

            assertNotNull(dataStructure);

            dataStructure.close();
        }
    }
}
