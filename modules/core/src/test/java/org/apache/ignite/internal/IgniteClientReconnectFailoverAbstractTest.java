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

package org.apache.ignite.internal;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;

/**
 *
 */
public abstract class IgniteClientReconnectFailoverAbstractTest extends IgniteClientReconnectAbstractTest {
    /** */
    private static final Integer THREADS = 1;

    /** */
    private volatile CyclicBarrier barrier;

    /** */
    protected static final long TEST_TIME = 90_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setJoinTimeout(30_000);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int serverCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected int clientCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIME * 60_000;
    }

    /**
     * @param c Test closure.
     * @throws Exception If failed.
     */
    protected final void reconnectFailover(final Callable<Void> c) throws Exception {
        final Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

        TestTcpDiscoverySpi srvSpi = spi(srv);

        final AtomicBoolean stop = new AtomicBoolean(false);

        final IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try {
                    int iter = 0;

                    while (!stop.get()) {
                        try {
                            c.call();
                        }
                        catch (CacheException e) {
                            checkAndWait(e);
                        }
                        catch (IgniteClientDisconnectedException e) {
                            checkAndWait(e);
                        }

                        if (++iter % 100 == 0)
                            log.info("Iteration: " + iter);

                        if (barrier != null)
                            barrier.await();
                    }

                    return null;
                }
                catch (Throwable e) {
                    log.error("Unexpected error in operation thread: " + e, e);

                    stop.set(true);

                    throw e;
                }
            }
        }, THREADS, "test-operation-thread");

        final AtomicReference<CountDownLatch> disconnected = new AtomicReference<>();
        final AtomicReference<CountDownLatch> reconnected = new AtomicReference<>();

        IgnitePredicate<Event> p = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    info("Reconnected: " + evt);

                    CountDownLatch latch = reconnected.get();

                    assertNotNull(latch);
                    assertEquals(1, latch.getCount());

                    latch.countDown();
                }
                else if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                    info("Disconnected: " + evt);

                    CountDownLatch latch = disconnected.get();

                    assertNotNull(latch);
                    assertEquals(1, latch.getCount());

                    latch.countDown();
                }

                return true;
            }
        };

        client.events().localListen(p, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);

        try {
            long stopTime = System.currentTimeMillis() + TEST_TIME;

            String err = null;

            while (System.currentTimeMillis() < stopTime && !fut.isDone()) {
                U.sleep(500);

                CountDownLatch disconnectLatch = new CountDownLatch(1);
                CountDownLatch reconnectLatch = new CountDownLatch(1);

                disconnected.set(disconnectLatch);
                reconnected.set(reconnectLatch);

                UUID nodeId = client.cluster().localNode().id();

                log.info("Fail client: " + nodeId);

                srvSpi.failNode(nodeId, null);

                if (!disconnectLatch.await(10_000, MILLISECONDS)) {
                    err = "Failed to wait for disconnect";

                    break;
                }

                if (!reconnectLatch.await(10_000, MILLISECONDS)) {
                    err = "Failed to wait for reconnect";

                    break;
                }

                barrier = new CyclicBarrier(THREADS + 1, new Runnable() {
                    @Override public void run() {
                        barrier = null;
                    }
                });

                try {
                    barrier.await(10, SECONDS);
                }
                catch (TimeoutException ignored) {
                    err = "Operations hang or fail with unexpected error.";

                    break;
                }
            }

            if (err != null) {
                log.error(err);

                U.dumpThreads(log);

                CyclicBarrier barrier0 = barrier;

                if (barrier0 != null)
                    barrier0.reset();

                stop.set(true);

                fut.get();

                fail(err);
            }

            stop.set(true);

            fut.get();
        }
        finally {
            client.events().stopLocalListen(p);

            stop.set(true);
        }
    }
}
