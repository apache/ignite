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

package org.apache.ignite.internal.processors.timeout;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Timeout processor tests.
 */
public class GridTimeoutProcessorSelfTest extends GridCommonAbstractTest {
    /** Random number generator. */
    private static final Random RAND = new Random();

    /** Kernal context. */
    private GridTestKernalContext ctx;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ctx = newContext();

        ctx.add(new GridTimeoutProcessor(ctx));

        ctx.start();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ctx.stop(true);

        ctx = null;
    }

    /**
     * Tests timeouts.
     *
     * @throws Exception If test failed.
     */
    public void testTimeouts() throws Exception {
        int max = 100;

        final CountDownLatch latch = new CountDownLatch(max);

        final Collection<GridTimeoutObject> timeObjs = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < max; i++) {
            final int idx = i;

            ctx.timeout().addTimeoutObject(new GridTimeoutObject() {
                /** Timeout ID. */
                private final IgniteUuid id = IgniteUuid.randomUuid();

                /** End time. */
                private final long endTime = System.currentTimeMillis() + RAND.nextInt(1000);

                /** {@inheritDoc} */
                @Override public IgniteUuid timeoutId() {
                    return id;
                }

                /** {@inheritDoc} */
                @Override public long endTime() { return endTime; }

                /** {@inheritDoc} */
                @Override public void onTimeout() {
                    info("Received timeout callback: " + this);

                    long now = System.currentTimeMillis();

                    if (now < endTime) {
                        fail("Timeout event happened prematurely [endTime=" + endTime + ", now=" + now +
                            ", obj=" + this + ']');
                    }

                    synchronized (timeObjs) {
                        timeObjs.add(this);
                    }

                    latch.countDown();
                }

                /** {@inheritDoc} */
                @Override public String toString() {
                    return "Timeout test object [idx=" + idx + ", endTime=" + endTime + ", id=" + id + ']';
                }
            });
        }

        latch.await();

        assert timeObjs.size() == max;

        // Ensure proper timeout sequence.
        long endTime = 0;

        for (GridTimeoutObject obj : timeObjs) {
            assert endTime <= obj.endTime();
            endTime = obj.endTime();
        }
    }

    /**
     * Multithreaded timeout test.
     *
     * @throws Exception If test failed.
     */
    public void testTimeoutsMultithreaded() throws Exception {
        final int max = 100;

        int threads = 20;

        final CountDownLatch latch = new CountDownLatch(max * threads);

        final Collection<GridTimeoutObject> timeObjs = new ConcurrentLinkedQueue<>();

        GridTestUtils.runMultiThreaded(new Runnable() {
            /** {@inheritDoc} */
            @Override public void run() {
                for (int i = 0; i < max; i++) {
                    final int idx = i;

                    ctx.timeout().addTimeoutObject(new GridTimeoutObject() {
                        /** Timeout ID. */
                        private final IgniteUuid id = IgniteUuid.randomUuid();

                        /** End time. */
                        private final long endTime = System.currentTimeMillis() + RAND.nextInt(1000) + 500;

                        /** {@inheritDoc} */
                        @Override public IgniteUuid timeoutId() { return id; }

                        /** {@inheritDoc} */
                        @Override public long endTime() { return endTime; }

                        /** {@inheritDoc} */
                        @Override public void onTimeout() {
                            long now = System.currentTimeMillis();

                            if (now < endTime) {
                                fail("Timeout event happened prematurely [endTime=" + endTime + ", now=" + now +
                                    ", obj=" + this + ']');
                            }

                            // This method will only be called from one thread, no synchronization required.
                            timeObjs.add(this);

                            latch.countDown();
                        }

                        /** {@inheritDoc} */
                        @Override public String toString() {
                            return "Timeout test object [idx=" + idx + ", endTime=" + endTime + ", id=" + id + ']';
                        }
                    });
                }
            }
        }, threads, "timeout-test-worker");

        latch.await();

        assert timeObjs.size() == max * threads;

        // Ensure proper timeout sequence.
        long endTime = 0;

        for (GridTimeoutObject obj : timeObjs) {
            assert endTime <= obj.endTime() : "Sequence check failed [endTime=" + endTime + ", obj=" + obj +
                ", objs=" + timeObjs + ']';

            endTime = obj.endTime();
        }
    }

    /**
     * Multithreaded timeout test with adapter.
     *
     * @throws Exception If test failed.
     */
    public void testTimeoutObjectAdapterMultithreaded() throws Exception {
        final int max = 100;

        int threads = 20;

        final CountDownLatch latch = new CountDownLatch(max * threads);

        final Collection<GridTimeoutObject> timeObjs = new ConcurrentLinkedQueue<>();

        GridTestUtils.runMultiThreaded(new Runnable() {
            /** {@inheritDoc} */
            @Override public void run() {
                for (int i = 0; i < max; i++) {
                    final int idx = i;

                    ctx.timeout().addTimeoutObject(new GridTimeoutObjectAdapter(RAND.nextInt(1000) + 500) {
                        @Override public void onTimeout() {
                            long now = System.currentTimeMillis();

                            if (now < endTime()) {
                                fail("Timeout event happened prematurely [endTime=" + endTime() + ", now=" + now +
                                    ", obj=" + this + ']');
                            }

                            // This method will only be called from one thread, no synchronization required.
                            timeObjs.add(this);

                            latch.countDown();
                        }

                        /** {@inheritDoc} */
                        @Override public String toString() {
                            return "Timeout test object [idx=" + idx + ", endTime=" + endTime() + ", id=" +
                                timeoutId() + ']';
                        }
                    });
                }
            }
        }, threads, "timeout-test-worker");

        latch.await();

        assert timeObjs.size() == max * threads;

        // Ensure proper timeout sequence.
        long endTime = 0;

        for (GridTimeoutObject obj : timeObjs) {
            assert endTime <= obj.endTime() : "Sequence check failed [endTime=" + endTime + ", obj=" + obj +
                ", objs=" + timeObjs + ']';

            endTime = obj.endTime();
        }
    }

    /**
     * Tests that timeout callback is never called.
     *
     * @throws Exception If test failed.
     */
    public void testTimeoutNeverCalled() throws Exception {
        int max = 100;

        final AtomicInteger callCnt = new AtomicInteger(0);

        Collection<GridTimeoutObject> timeObjs = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < max; i++) {
            final int idx = i;

            GridTimeoutObject obj = new GridTimeoutObject() {
                /** Timeout ID. */
                private final IgniteUuid id = IgniteUuid.randomUuid();

                /** End time. */
                private final long endTime = System.currentTimeMillis() + RAND.nextInt(500) + 500;

                /** {@inheritDoc} */
                @Override public IgniteUuid timeoutId() {
                    return id;
                }

                /** {@inheritDoc} */
                @Override public long endTime() {
                    return endTime;
                }

                /** {@inheritDoc} */
                @Override public void onTimeout() {
                    callCnt.incrementAndGet();
                }

                /** {@inheritDoc} */
                @Override public String toString() {
                    return "Timeout test object [idx=" + idx + ", endTime=" + endTime + ", id=" + id + ']';
                }
            };

            timeObjs.add(obj);

            ctx.timeout().addTimeoutObject(obj);
        }

        assert timeObjs.size() == max;

        // Remove timeout objects so that they aren't able to times out (supposing the cycle takes less than 500 ms).
        for (GridTimeoutObject obj : timeObjs) {
            ctx.timeout().removeTimeoutObject(obj);
        }

        Thread.sleep(1000);

        assert callCnt.get() == 0;
    }

    /**
     * Tests that timeout callback is never called.
     *
     * @throws Exception If test failed.
     */
    public void testTimeoutNeverCalledMultithreaded() throws Exception {

        int threads = 20;

        final AtomicInteger callCnt = new AtomicInteger(0);

        final Collection<GridTimeoutObject> timeObjs = new ConcurrentLinkedQueue<>();

        GridTestUtils.runMultiThreaded(new Runnable() {
            /** {@inheritDoc} */
            @Override public void run() {
                int max = 100;

                for (int i = 0; i < max; i++) {
                    final int idx = i;

                    GridTimeoutObject obj = new GridTimeoutObject() {
                        /** Timeout ID. */
                        private final IgniteUuid id = IgniteUuid.randomUuid();

                        /** End time. */
                        private final long endTime = System.currentTimeMillis() + RAND.nextInt(500) + 500;

                        /** {@inheritDoc} */
                        @Override public IgniteUuid timeoutId() {
                            return id;
                        }

                        /** {@inheritDoc} */
                        @Override public long endTime() { return endTime; }

                        /** {@inheritDoc} */
                        @Override public void onTimeout() {
                            callCnt.incrementAndGet();
                        }

                        /** {@inheritDoc} */
                        @Override public String toString() {
                            return "Timeout test object [idx=" + idx + ", endTime=" + endTime + ", id=" + id + ']';
                        }
                    };

                    timeObjs.add(obj);

                    ctx.timeout().addTimeoutObject(obj);
                }

                // Remove timeout objects so that they aren't able to times out
                // (supposing the cycle takes less than 500 ms).
                for (GridTimeoutObject obj : timeObjs) {
                    ctx.timeout().removeTimeoutObject(obj);
                }
            }
        }, threads, "timeout-test-worker");

        Thread.sleep(1000);

        assert callCnt.get() == 0;
    }

    public void testAddRemoveInterleaving() throws Exception {
        final AtomicInteger callCnt = new AtomicInteger(0);

        IgniteInternalFuture<?> rmv = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            /** {@inheritDoc} */
            @SuppressWarnings("CallToThreadYield")
            @Override public void run() {
                final Collection<GridTimeoutObject> timeObjs = new ConcurrentLinkedQueue<>();

                for (int i = 0; i < 1000; i++) {
                    final int idx = i;

                    GridTimeoutObject obj = new GridTimeoutObject() {
                        /** Timeout ID. */
                        private final IgniteUuid id = IgniteUuid.randomUuid();

                        /** End time. */
                        private final long endTime = System.currentTimeMillis() + RAND.nextInt(500) + 1000;

                        /** {@inheritDoc} */
                        @Override public IgniteUuid timeoutId() {
                            return id;
                        }

                        /** {@inheritDoc} */
                        @Override public long endTime() {
                            return endTime;
                        }

                        /** {@inheritDoc} */
                        @Override public void onTimeout() {
                            callCnt.incrementAndGet();
                        }

                        /** {@inheritDoc} */
                        @Override public String toString() {
                            return "Timeout test object [idx=" + idx + ", endTime=" + endTime + ", id=" + id + ']';
                        }
                    };

                    timeObjs.add(obj);

                    ctx.timeout().addTimeoutObject(obj);

                    Thread.yield();
                }

                // Remove timeout objects so that they aren't able to times out
                // (supposing the cycle takes less than 500 ms).
                for (GridTimeoutObject obj : timeObjs) {
                    ctx.timeout().removeTimeoutObject(obj);
                }
            }
        }, 100, "timeout-test-worker");

        final int max = 1000;

        int threads = 50;

        final CountDownLatch latch = new CountDownLatch(max * threads);

        IgniteInternalFuture<?> called = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            /** {@inheritDoc} */
            @SuppressWarnings("CallToThreadYield")
            @Override public void run() {
                for (int i = 0; i < max; i++) {
                    final int idx = i;

                    GridTimeoutObject obj = new GridTimeoutObject() {
                        /** Timeout ID. */
                        private final IgniteUuid id = IgniteUuid.randomUuid();

                        /** End time. */
                        private final long endTime = System.currentTimeMillis() + RAND.nextInt(500) + 500;

                        /** {@inheritDoc} */
                        @Override public IgniteUuid timeoutId() {
                            return id;
                        }

                        /** {@inheritDoc} */
                        @Override public long endTime() {
                            return endTime;
                        }

                        /** {@inheritDoc} */
                        @Override public void onTimeout() {
                            long now = System.currentTimeMillis();

                            if (now < endTime) {
                                fail("Timeout event happened prematurely [endTime=" + endTime + ", now=" + now +
                                    ", obj=" + this + ']');
                            }

                            latch.countDown();
                        }

                        /** {@inheritDoc} */
                        @Override public String toString() {
                            return "Timeout test object [idx=" + idx + ", endTime=" + endTime + ", id=" + id + ']';
                        }
                    };

                    ctx.timeout().addTimeoutObject(obj);

                    Thread.yield();
                }
            }
        }, threads, "timeout-test-worker");

        rmv.get();
        called.get();

        latch.await();

        assert callCnt.get() == 0;
    }

    /**
     * Tests that timeout objects times out only once.
     *
     * @throws Exception If test failed.
     */
    public void testTimeoutCallOnce() throws Exception {
        ctx.timeout().addTimeoutObject(new GridTimeoutObject() {
            /** Timeout ID. */
            private final IgniteUuid id = IgniteUuid.randomUuid();

            /** End time. */
            private final long endTime = System.currentTimeMillis() + RAND.nextInt(500) + 100;

            /** Number of calls. */
            private int cnt;

            /** {@inheritDoc} */
            @Override public IgniteUuid timeoutId() { return id; }

            /** {@inheritDoc} */
            @Override public long endTime() { return endTime; }

            /** {@inheritDoc} */
            @Override public void onTimeout() {
                info("Received timeout callback: " + this);

                if (++cnt > 1)
                    fail("Timeout should not be called more than once: " + this);
            }

            /** {@inheritDoc} */
            @Override public String toString() {
                return "Timeout test object [endTime=" + endTime + ", id=" + id + ']';
            }
        });

        Thread.sleep(2000);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testTimeoutSameEndTime() throws Exception {
        final CountDownLatch latch = new CountDownLatch(2);

        final long endTime0 = System.currentTimeMillis() + 1000;

        ctx.timeout().addTimeoutObject(new GridTimeoutObject() {
            /** Timeout ID. */
            private final IgniteUuid id = IgniteUuid.randomUuid();

            /** End time. */
            private final long endTime = endTime0;

            /** {@inheritDoc} */
            @Override public IgniteUuid timeoutId() {
                return id;
            }

            /** {@inheritDoc} */
            @Override public long endTime() {
                return endTime;
            }

            /** {@inheritDoc} */
            @Override public void onTimeout() {
                info("Received timeout callback: " + this);

                latch.countDown();
            }

            /** {@inheritDoc} */
            @Override public String toString() {
                return "Timeout test object [endTime=" + endTime + ", id=" + id + ']';
            }
        });

        ctx.timeout().addTimeoutObject(new GridTimeoutObject() {
            /** Timeout ID. */
            private final IgniteUuid id = IgniteUuid.randomUuid();

            /** End time. */
            private final long endTime = endTime0;

            /** {@inheritDoc} */
            @Override public IgniteUuid timeoutId() {
                return id;
            }

            /** {@inheritDoc} */
            @Override public long endTime() {
                return endTime;
            }

            /** {@inheritDoc} */
            @Override public void onTimeout() {
                info("Received timeout callback: " + this);

                latch.countDown();
            }

            /** {@inheritDoc} */
            @Override public String toString() {
                return "Timeout test object [endTime=" + endTime + ", id=" + id + ']';
            }
        });

        assert latch.await(3000, MILLISECONDS);
    }
}