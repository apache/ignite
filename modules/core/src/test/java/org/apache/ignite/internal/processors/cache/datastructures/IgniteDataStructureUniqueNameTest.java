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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

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
    @Override protected CacheMemoryMode collectionMemoryMode() {
        return ONHEAP_TIERED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode collectionCacheAtomicityMode() {
        return ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        AtomicConfiguration atomicCfg = new AtomicConfiguration();

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
                ignite.queue(name, 0, config(false));

                return null;
            }
        }, IgniteException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite.queue(name, 0, null);

                return null;
            }
        }, IgniteException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite.set(name, config(false));

                return null;
            }
        }, IgniteException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite.set(name, null);

                return null;
            }
        }, IgniteException.class, null);

        atomicLong.close();

        IgniteQueue<Integer> q = ignite.queue(name, 0, config(false));

        assertNotNull(q);

        assertSame(q, ignite.queue(name, 0, config(false)));

        assertSame(q, ignite.queue(name, 0, null));

        q.close();

        assertNull(ignite.set(name, null));

        IgniteSet<Integer> set = ignite.set(name, config(false));

        assertNotNull(set);

        assertSame(set, ignite.set(name, config(false)));

        assertSame(set, ignite.set(name, null));

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite.atomicReference(name, 0, false);

                return null;
            }
        }, IgniteException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite.queue(name, 0, config(false));

                return null;
            }
        }, IgniteException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ignite.queue(name, 0, null);

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

        final int DS_TYPES = 7;

        final int THREADS = DS_TYPES * 3;

        for (int iter = 0; iter < 20; iter++) {
            log.info("Iteration: " + iter);

            List<IgniteInternalFuture<Object>> futs = new ArrayList<>(THREADS);

            final CyclicBarrier barrier = new CyclicBarrier(THREADS);

            for (int i = 0; i < THREADS; i++) {
                final int idx = i;

                IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try {
                            Thread.currentThread().setName("test thread-" + idx);

                            barrier.await();

                            Ignite ignite = singleGrid ? ignite(0) : ignite(idx % gridCount());

                            Object res;

                            switch (idx % DS_TYPES) {
                                case 0:
                                    log.info("Create atomic long, grid: " + ignite.name());

                                    res = ignite.atomicLong(name, 0, true);

                                    break;

                                case 1:
                                    log.info("Create atomic sequence, grid: " + ignite.name());

                                    res = ignite.atomicSequence(name, 0, true);

                                    break;

                                case 2:
                                    log.info("Create atomic stamped, grid: " + ignite.name());

                                    res = ignite.atomicStamped(name, 0, true, true);

                                    break;

                                case 3:
                                    log.info("Create atomic latch, grid: " + ignite.name());

                                    res = ignite.countDownLatch(name, 0, true, true);

                                    break;

                                case 4:
                                    log.info("Create atomic reference, grid: " + ignite.name());

                                    res = ignite.atomicReference(name, null, true);

                                    break;

                                case 5:
                                    log.info("Create queue, grid: " + ignite.name());

                                    res = ignite.queue(name, 0, config(false));

                                    break;

                                case 6:
                                    log.info("Create set, grid: " + ignite.name());

                                    res = ignite.set(name, config(false));

                                    break;

                                default:
                                    fail();

                                    return null;
                            }

                            log.info("Thread created: " + res);

                            return res;
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

            int createdCnt = 0;

            for (IgniteInternalFuture<Object> fut : futs) {
                Object res = fut.get();

                if (res instanceof IgniteException)
                    continue;

                assertTrue("Unexpected object: " + res,
                    res instanceof IgniteAtomicLong ||
                        res instanceof IgniteAtomicSequence ||
                        res instanceof IgniteAtomicReference ||
                        res instanceof IgniteAtomicStamped ||
                        res instanceof IgniteCountDownLatch ||
                        res instanceof IgniteQueue ||
                        res instanceof IgniteSet);

                log.info("Data structure created: " + dataStructure);

                createdCnt++;

                if (dataStructure != null)
                    assertEquals(dataStructure.getClass(), res.getClass());
                else
                    dataStructure = (Closeable)res;
            }

            assertNotNull(dataStructure);

            assertEquals(3, createdCnt);

            dataStructure.close();
        }
    }
}