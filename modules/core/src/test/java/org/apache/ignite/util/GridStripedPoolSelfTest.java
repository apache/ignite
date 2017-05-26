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

package org.apache.ignite.util;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import junit.framework.TestCase;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.GridStripedPool;
import org.apache.ignite.testframework.GridTestUtils;
import org.jsr166.LongAdder8;

/**
 */
public class GridStripedPoolSelfTest extends TestCase {
    /**
     * @throws Exception If failed.
     */
    public void testStripedPool() throws Exception {
        final TestPool pool = new TestPool();

        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicBoolean disableBrokenIn = new AtomicBoolean();

        final LongAdder8 broken = new LongAdder8();

        final int threads = 16;

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                GridRandom rnd = new GridRandom();

                while (!stop.get()) {
                    AtomicReference<State> o = pool.take();

                    switch (o.get()) {
                        case BROKEN_IN:
                            // Just return to the pool and it must be destroyed there.
                            break;

                        case CLEAN:
                            if (rnd.nextBoolean()) {
                                // May be make it dirty.
                                if (!o.compareAndSet(State.CLEAN, State.DIRTY))
                                    assertEquals(State.BROKEN_IN, o.get());
                            }
                            else if (rnd.nextInt(10) == 0) {
                                if (o.compareAndSet(State.CLEAN, State.BROKEN_OUT))
                                    broken.increment();
                                else
                                    assertEquals(State.BROKEN_IN, o.get());
                            }

                            break;

                        default:
                            fail("State: " + o.get());
                    }

                    pool.put(o);

                    // Break the object inside of the pool.
                    if (rnd.nextInt(15) == 0 && !disableBrokenIn.get()) {
                        if (o.compareAndSet(State.CLEAN, State.BROKEN_IN))
                            broken.increment();
                    }
                }
            }
        }, threads, "test-run");

        Thread.sleep(3000);

        disableBrokenIn.set(true);

        Thread.sleep(500);

        stop.set(true);

        fut.get();

        assertTrue(pool.getPoolSize() > 0);
        assertEquals(pool.created.sum() - pool.destroyed.sum(), pool.getPoolSize());
        assertEquals(broken.sum(), pool.destroyed.sum());

        pool.close();

        assertEquals(0, pool.getPoolSize());
        assertEquals(pool.created.sum(), pool.destroyed.sum());
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentClose() throws Exception {
        final TestPool pool = new TestPool();

        final int threads = 16;

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                for (;;) {
                    AtomicReference<State> o;

                    try {
                        o = pool.take();
                    }
                    catch (IllegalStateException e) {
                        assertTrue(pool.isClosed());

                        return;
                    }

                    assertTrue(o.compareAndSet(State.CLEAN, State.DIRTY));

                    pool.put(o);
                }
            }
        }, threads, "test-run");

        Thread.sleep(500);

        assertFalse(pool.isClosed());
        pool.close();
        assertTrue(pool.isClosed());

        fut.get();

        assertEquals(0, pool.getPoolSize());
        assertEquals(pool.created.sum(), pool.destroyed.sum());
    }

    /**
     */
    private static class TestPool extends GridStripedPool<AtomicReference<State>, RuntimeException> {
        /** */
        private LongAdder8 created = new LongAdder8();

        /** */
        private LongAdder8 destroyed = new LongAdder8();

        /**
         */
        public TestPool() {
            super(4, 3);
        }

        /** {@inheritDoc} */
        @Override protected boolean validate(AtomicReference<State> o) throws RuntimeException {
            return o.get() == State.CLEAN;
        }

        /** {@inheritDoc} */
        @Override protected AtomicReference<State> create() throws RuntimeException {
            created.increment();

            return new AtomicReference<>(State.CLEAN);
        }

        /** {@inheritDoc} */
        @Override protected void cleanup(AtomicReference<State> o) throws RuntimeException {
            o.compareAndSet(State.DIRTY, State.CLEAN);
        }

        /** {@inheritDoc} */
        @Override protected void destroy(AtomicReference<State> o) throws RuntimeException {
            o.set(State.DESTROYED);

            assertEquals(State.DESTROYED, o.get());

            destroyed.increment();
        }
    }

    /**
     */
    private enum State {
        CLEAN, DIRTY, BROKEN_OUT, BROKEN_IN, DESTROYED
    }
}
