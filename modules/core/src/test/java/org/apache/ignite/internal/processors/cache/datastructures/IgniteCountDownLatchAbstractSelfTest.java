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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Cache count down latch self test.
 */
public abstract class IgniteCountDownLatchAbstractSelfTest extends IgniteAtomicsAbstractTest
    implements Externalizable {
    /** */
    private static final int NODES_CNT = 4;

    /** */
    protected static final int THREADS_CNT = 5;

    /** */
    private static final Random RND = new Random();

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return NODES_CNT;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLatch() throws Exception {
        checkLatch();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkLatch() throws Exception {
        // Test API.
        checkAutoDelete();

        checkAwait();

        checkCountDown();

        // Test main functionality.
        IgniteCountDownLatch latch1 = grid(0).countDownLatch("latch", 2, false, true);

        assertEquals(2, latch1.count());

        IgniteCompute comp = grid(0).compute().withAsync();

        comp.call(new IgniteCallable<Object>() {
            @IgniteInstanceResource
            private Ignite ignite;

            @LoggerResource
            private IgniteLogger log;

            @Nullable @Override public Object call() throws Exception {
                // Test latch in multiple threads on each node.
                IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(
                    new Callable<Object>() {
                        @Nullable @Override public Object call() throws Exception {
                            IgniteCountDownLatch latch = ignite.countDownLatch("latch", 2, false, true);

                            assert latch != null && latch.count() == 2;

                            log.info("Thread is going to wait on latch: " + Thread.currentThread().getName());

                            assert latch.await(1, MINUTES);

                            log.info("Thread is again runnable: " + Thread.currentThread().getName());

                            return null;
                        }
                    },
                    5,
                    "test-thread"
                );

                fut.get();

                return null;
            }
        });

        IgniteFuture<Object> fut = comp.future();

        Thread.sleep(3000);

        assert latch1.countDown() == 1;

        assert latch1.countDown() == 0;

        // Ensure there are no hangs.
        fut.get();

        // Test operations on removed latch.
        latch1.close();

        checkRemovedLatch(latch1);
    }

    /**
     * @param latch Latch.
     *
     * @throws Exception If failed.
     */
    protected void checkRemovedLatch(final IgniteCountDownLatch latch) throws Exception {
        assert GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return latch.removed();
            }
        }, 5000);

        assert latch.removed();

        assert latch.count() == 0;

        // Test await on removed future.
        latch.await();
        assert latch.await(10);
        assert latch.await(10, SECONDS);

        latch.await();

        // Test countdown.
        assert latch.countDown() == 0;
        assert latch.countDown(5) == 0;
        latch.countDownAll();
    }

    /**
     * @throws Exception Exception.
     */
    private void checkAutoDelete() throws Exception {
        IgniteCountDownLatch latch = createLatch("rmv", 5, true);

        latch.countDownAll();

        // Latch should be removed since autoDelete = true
        checkRemovedLatch(latch);

        IgniteCountDownLatch latch1 = createLatch("rmv1", 5, false);

        latch1.countDownAll();

        // Latch should NOT be removed since autoDelete = false
        assert !latch1.removed();

        removeLatch("rmv1");
    }

    /**
     * @throws Exception Exception.
     */
    private void checkAwait() throws Exception {
        // Check only 'false' cases here. Successful await is tested over the grid.
        IgniteCountDownLatch latch = createLatch("await", 5, false);

        assert !latch.await(10);
        assert !latch.await(10, MILLISECONDS);

        removeLatch("await");
    }

    /**
     * @throws Exception Exception.
     */
    private void checkCountDown() throws Exception {
        IgniteCountDownLatch latch = createLatch("cnt", 10, true);

        assert latch.countDown() == 9;
        assert latch.countDown(2) == 7;

        latch.countDownAll();

        assert latch.count() == 0;

        checkRemovedLatch(latch);

        IgniteCountDownLatch latch1 = createLatch("cnt1", 10, true);

        assert latch1.countDown() == 9;
        assert latch1.countDown(2) == 7;

        latch1.countDownAll();

        assert latch1.count() == 0;

        checkRemovedLatch(latch1);
    }

    /**
     * @param latchName Latch name.
     * @param cnt Count.
     * @param autoDel Auto delete flag.
     * @throws Exception If failed.
     * @return New latch.
     */
    private IgniteCountDownLatch createLatch(String latchName, int cnt, boolean autoDel)
        throws Exception {
        IgniteCountDownLatch latch = grid(RND.nextInt(NODES_CNT)).countDownLatch(latchName, cnt, autoDel, true);

        // Test initialization.
        assert latchName.equals(latch.name());
        assert latch.count() == cnt;
        assert latch.initialCount() == cnt;
        assert latch.autoDelete() == autoDel;

        return latch;
    }

    /**
     * @param latchName Latch name.
     * @throws Exception If failed.
     */
    private void removeLatch(String latchName)
        throws Exception {
        IgniteCountDownLatch latch = grid(RND.nextInt(NODES_CNT)).countDownLatch(latchName, 10, false, true);

        assert latch != null;

        if (latch.count() > 0)
            latch.countDownAll();

        // Remove latch on random node.
        IgniteCountDownLatch latch0 = grid(RND.nextInt(NODES_CNT)).countDownLatch(latchName, 0, false, false);

        assertNotNull(latch0);

        latch0.close();

        // Ensure latch is removed on all nodes.
        for (Ignite g : G.allGrids())
            assertNull(((IgniteKernal)g).context().dataStructures().countDownLatch(latchName, 10, true, false));

        checkRemovedLatch(latch);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLatchMultinode1() throws Exception {
        if (gridCount() == 1)
            return;

        IgniteCountDownLatch latch = grid(0).countDownLatch("l1", 10,
            true,
            true);

        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        final AtomicBoolean countedDown = new AtomicBoolean();

        for (int i = 0; i < gridCount(); i++) {
            final Ignite ignite = grid(i);

            futs.add(GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteCountDownLatch latch = ignite.countDownLatch("l1", 10,
                        true,
                        false);

                    assertNotNull(latch);

                    boolean wait = latch.await(30_000);

                    assertTrue(countedDown.get());

                    assertEquals(0, latch.count());

                    assertTrue(wait);

                    return null;
                }
            }));
        }

        for (int i = 0; i < 10; i++) {
            if (i == 9)
                countedDown.set(true);

            latch.countDown();
        }

        for (IgniteInternalFuture<?> fut : futs)
            fut.get(30_000);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLatchMultinode2() throws Exception {
        if (gridCount() == 1)
            return;

        IgniteCountDownLatch latch = grid(0).countDownLatch("l2", gridCount() * 3,
            true,
            true);

        assertNotNull(latch);

        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        final AtomicInteger cnt = new AtomicInteger();

        for (int i = 0; i < gridCount(); i++) {
            final Ignite ignite = grid(i);

            futs.add(GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteCountDownLatch latch = ignite.countDownLatch("l2", 10,
                        true,
                        false);

                    assertNotNull(latch);

                    for (int i = 0; i < 3; i++) {
                        cnt.incrementAndGet();

                        latch.countDown();
                    }

                    boolean wait = latch.await(30_000);

                    assertEquals(gridCount() * 3, cnt.get());

                    assertEquals(0, latch.count());

                    assertTrue(wait);

                    return null;
                }
            }));
        }

        for (IgniteInternalFuture<?> fut : futs)
            fut.get(30_000);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // No-op.
    }
}