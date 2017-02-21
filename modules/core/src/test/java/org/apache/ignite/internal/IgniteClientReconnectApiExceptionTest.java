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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import javax.cache.CacheException;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;

/**
 *
 */
public class IgniteClientReconnectApiExceptionTest extends IgniteClientReconnectAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(new CacheConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int serverCount() {
        return 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testErrorOnDisconnect() throws Exception {
        // Check cache operations.
        cacheOperationsTest();

        // Check cache operations.
        beforeTestsStarted();
        dataStructureOperationsTest();

        // Check ignite operations.
        beforeTestsStarted();
        igniteOperationsTest();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void dataStructureOperationsTest() throws Exception {
        clientMode = true;

        final Ignite client = startGrid(serverCount());

        doTestIgniteOperationOnDisconnect(client, Arrays.asList(
            // Check atomic long.
            new T2<Callable, C1<Object, Boolean>>(
                new Callable() {
                    @Override public Object call() throws Exception {
                        boolean failed = false;

                        try {
                            client.atomicLong("testAtomic", 41, true);
                        }
                        catch (IgniteClientDisconnectedException e) {
                            failed = true;

                            checkAndWait(e);
                        }

                        assertTrue(failed);

                        return client.atomicLong("testAtomic", 41, true);
                    }
                },
                new C1<Object, Boolean>() {
                    @Override public Boolean apply(Object o) {
                        assertNotNull(o);

                        IgniteAtomicLong atomicLong = (IgniteAtomicLong)o;

                        assertEquals(42, atomicLong.incrementAndGet());

                        return true;
                    }
                }
            ),
            // Check set.
            new T2<Callable, C1<Object, Boolean>>(
                new Callable() {
                    @Override public Object call() throws Exception {
                        boolean failed = false;

                        try {
                            client.set("testSet", new CollectionConfiguration());
                        }
                        catch (IgniteClientDisconnectedException e) {
                            failed = true;

                            checkAndWait(e);
                        }

                        assertTrue(failed);

                        return client.set("testSet", new CollectionConfiguration());
                    }
                },
                new C1<Object, Boolean>() {
                    @Override public Boolean apply(Object o) {
                        assertNotNull(o);

                        IgniteSet set = (IgniteSet)o;

                        String val = "testVal";

                        set.add(val);

                        assertEquals(1, set.size());
                        assertTrue(set.contains(val));

                        return true;
                    }
                }
            ),
            // Check ignite queue.
            new T2<Callable, C1<Object, Boolean>>(
                new Callable() {
                    @Override public Object call() throws Exception {
                        boolean failed = false;

                        try {
                            client.queue("TestQueue", 10, new CollectionConfiguration());
                        }
                        catch (IgniteClientDisconnectedException e) {
                            failed = true;

                            checkAndWait(e);
                        }

                        assertTrue(failed);

                        return client.queue("TestQueue", 10, new CollectionConfiguration());
                    }
                },
                new C1<Object, Boolean>() {
                    @Override public Boolean apply(Object o) {
                        assertNotNull(o);

                        IgniteQueue queue = (IgniteQueue)o;

                        String val = "Test";

                        queue.add(val);

                        assertEquals(val, queue.poll());

                        return true;
                    }
                }
            )
        ));

        clientMode = false;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void cacheOperationsTest() throws Exception {
        clientMode = true;

        final Ignite client = startGrid(serverCount());

        final IgniteCache<Object, Object> dfltCache = client.cache(null);

        assertNotNull(dfltCache);

        doTestIgniteOperationOnDisconnect(client, Arrays.asList(
            // Check put and get operation.
            new T2<Callable, C1<Object, Boolean>>(
                new Callable() {
                    @Override public Object call() throws Exception {
                        boolean failed = false;

                        try {
                            dfltCache.getAndPut(9999, 9999);
                        }
                        catch (CacheException e) {
                            failed = true;

                            checkAndWait(e);
                        }

                        assertTrue(failed);

                        return dfltCache.getAndPut(9999, 9999);
                    }
                },
                new C1<Object, Boolean>() {
                    @Override public Boolean apply(Object o) {
                        assertNull(o);

                        assertEquals(9999, dfltCache.get(9999));

                        return true;
                    }
                }
            ),
            // Check put operation.
            new T2<Callable, C1<Object, Boolean>>(
                new Callable() {
                    @Override public Object call() throws Exception {
                        boolean failed = false;

                        try {
                            dfltCache.put(10000, 10000);
                        }
                        catch (CacheException e) {
                            failed = true;

                            checkAndWait(e);
                        }

                        assertTrue(failed);

                        dfltCache.put(10000, 10000);

                        return true;
                    }
                },
                new C1<Object, Boolean>() {
                    @Override public Boolean apply(Object o) {
                        assertTrue((Boolean)o);

                        assertEquals(10000, dfltCache.get(10000));

                        return true;
                    }
                }
            ),
            // Check get operation.
            new T2<Callable, C1<Object, Boolean>>(
                new Callable() {
                    @Override public Object call() throws Exception {
                        boolean failed = false;

                        try {
                            dfltCache.get(10001);
                        }
                        catch (CacheException e) {
                            failed = true;

                            checkAndWait(e);
                        }

                        assertTrue(failed);

                        return dfltCache.get(10001);
                    }
                },
                new C1<Object, Boolean>() {
                    @Override public Boolean apply(Object o) {
                        assertNull(o);

                        return true;
                    }
                }
            ),
            // Check invoke operation.
            new T2<Callable, C1<Object, Boolean>>(
                new Callable() {
                    @Override public Object call() throws Exception {
                        boolean failed = false;

                        try {
                            dfltCache.invoke(10000, new CacheEntryProcessor<Object, Object, Object>() {
                                @Override public Object process(MutableEntry<Object, Object> entry,
                                    Object... arguments) throws EntryProcessorException {
                                    assertTrue(entry.exists());

                                    return (int)entry.getValue() * 2;
                                }
                            });
                        }
                        catch (CacheException e) {
                            failed = true;

                            checkAndWait(e);
                        }

                        assertTrue(failed);

                        return dfltCache.invoke(10000, new CacheEntryProcessor<Object, Object, Object>() {
                            @Override public Object process(MutableEntry<Object, Object> entry,
                                Object... arguments) throws EntryProcessorException {
                                assertTrue(entry.exists());

                                return (int)entry.getValue() * 2;
                            }
                        });
                    }
                },
                new C1<Object, Boolean>() {
                    @Override public Boolean apply(Object o) {
                        assertNotNull(o);

                        assertEquals(20000, (int)o);

                        return true;
                    }
                }
            ),
            // Check put async operation.
            new T2<Callable, C1<Object, Boolean>>(
                new Callable() {
                    @Override public Object call() throws Exception {
                        IgniteCache<Object, Object> async = dfltCache.withAsync();

                        boolean failed = false;

                        try {
                            async.put(10002, 10002);

                            async.future().get();
                        }
                        catch (CacheException e) {
                            failed = true;

                            checkAndWait(e);
                        }

                        assertTrue(failed);

                        async.put(10002, 10002);

                        return async.future().get();
                    }
                },
                new C1<Object, Boolean>() {
                    @Override public Boolean apply(Object o) {
                        assertNull(o);

                        assertEquals(10002, dfltCache.get(10002));

                        return true;
                    }
                }
            ),
            // Check transaction.
            new T2<Callable, C1<Object, Boolean>>(
                new Callable() {
                    @Override public Object call() throws Exception {
                        boolean failed = false;

                        try {
                            client.transactions();
                        }
                        catch (IgniteClientDisconnectedException e) {
                            failed = true;

                            checkAndWait(e);
                        }

                        assertTrue(failed);

                        return client.transactions();
                    }
                },
                new C1<Object, Boolean>() {
                    @Override public Boolean apply(Object o) {
                        IgniteTransactions txs = (IgniteTransactions)o;

                        assertNotNull(txs);

                        return true;
                    }
                }
            ),
            // Check get cache.
            new T2<Callable, C1<Object, Boolean>>(
                new Callable() {
                    @Override public Object call() throws Exception {
                        boolean failed = false;

                        try {
                            client.cache(null);
                        }
                        catch (IgniteClientDisconnectedException e) {
                            failed = true;

                            checkAndWait(e);
                        }

                        assertTrue(failed);

                        return client.cache(null);
                    }
                },
                new C1<Object, Boolean>() {
                    @Override public Boolean apply(Object o) {
                        IgniteCache<Object, Object> cache0 = (IgniteCache<Object, Object>)o;

                        assertNotNull(cache0);

                        cache0.put(1, 1);

                        assertEquals(1, cache0.get(1));

                        return true;
                    }
                }
            ),
            // Check streamer.
            new T2<Callable, C1<Object, Boolean>>(
                new Callable() {
                    @Override public Object call() throws Exception {
                        boolean failed = false;

                        try {
                            client.dataStreamer(null);
                        }
                        catch (IgniteClientDisconnectedException e) {
                            failed = true;

                            checkAndWait(e);
                        }

                        assertTrue(failed);

                        return client.dataStreamer(null);
                    }
                },
                new C1<Object, Boolean>() {
                    @Override public Boolean apply(Object o) {
                        IgniteDataStreamer<Object, Object> streamer = (IgniteDataStreamer<Object, Object>)o;

                        streamer.addData(2, 2);

                        streamer.close();

                        assertEquals(2, client.cache(null).get(2));

                        return true;
                    }
                }
            ),
            // Check create cache.
            new T2<Callable, C1<Object, Boolean>>(
                new Callable() {
                    @Override public Object call() throws Exception {
                        boolean failed = false;

                        try {
                            client.createCache("test_cache");
                        }
                        catch (IgniteClientDisconnectedException e) {
                            failed = true;

                            checkAndWait(e);
                        }

                        assertTrue(failed);

                        return client.createCache("test_cache");
                    }
                },
                new C1<Object, Boolean>() {
                    @Override public Boolean apply(Object o) {
                        IgniteCache<Object, Object> cache = (IgniteCache<Object, Object>)o;

                        assertNotNull(cache);

                        cache.put(1, 1);

                        assertEquals(1, cache.get(1));

                        return true;
                    }
                }
            )

        ));

        clientMode = false;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void igniteOperationsTest() throws Exception {
        clientMode = true;

        final Ignite client = startGrid(serverCount());

        final IgniteCache<Object, Object> dfltCache = client.cache(null);

        final CountDownLatch recvLatch = new CountDownLatch(1);

        assertNotNull(dfltCache);

        doTestIgniteOperationOnDisconnect(client, Arrays.asList(
            // Check compute.
            new T2<Callable, C1<Object, Boolean>>(
                new Callable() {
                    @Override public Object call() throws Exception {
                        boolean failed = false;

                        try {
                            client.compute();
                        }
                        catch (IgniteClientDisconnectedException e) {
                            failed = true;

                            checkAndWait(e);
                        }

                        assertTrue(failed);

                        return client.compute();
                    }
                },
                new C1<Object, Boolean>() {
                    @Override public Boolean apply(Object o) {
                        IgniteCompute comp = (IgniteCompute)o;

                        Collection<UUID> uuids = comp.broadcast(new IgniteCallable<UUID>() {
                            @IgniteInstanceResource
                            private Ignite ignite;

                            @Override public UUID call() throws Exception {
                                return ignite.cluster().localNode().id();
                            }
                        });

                        assertFalse(uuids.isEmpty());

                        for (UUID uuid : uuids)
                            assertNotNull(uuid);

                        return true;
                    }
                }
            ),

            // Check ping node.
            new T2<Callable, C1<Object, Boolean>>(
                new Callable() {
                    @Override public Object call() throws Exception {
                        boolean failed = false;

                        try {
                            client.cluster().pingNode(new UUID(0, 0));
                        }
                        catch (IgniteClientDisconnectedException e) {
                            failed = true;

                            checkAndWait(e);
                        }

                        assertTrue(failed);

                        return client.cluster().pingNode(new UUID(0, 0));
                    }
                },
                new C1<Object, Boolean>() {
                    @Override public Boolean apply(Object o) {
                        Boolean pingNode = (Boolean)o;

                        assertFalse(pingNode);

                        return true;
                    }
                }
            ),
            // Check register remote listener.
            new T2<Callable, C1<Object, Boolean>>(
                new Callable() {
                    @Override public Object call() throws Exception {
                        boolean failed = false;

                        try {
                            client.events().remoteListen(null, new IgnitePredicate<Event>() {
                                @Override public boolean apply(Event event) {
                                    return true;
                                }
                            });
                        }
                        catch (IgniteClientDisconnectedException e) {
                            failed = true;

                            checkAndWait(e);
                        }

                        assertTrue(failed);

                        return client.events().remoteListen(null, new IgnitePredicate<Event>() {
                            @Override public boolean apply(Event event) {
                                return true;
                            }
                        });
                    }
                },
                new C1<Object, Boolean>() {
                    @Override public Boolean apply(Object o) {
                        UUID remoteId = (UUID)o;

                        assertNotNull(remoteId);

                        client.events().stopRemoteListen(remoteId);

                        return true;
                    }
                }
            ),
            // Check message operation.
            new T2<Callable, C1<Object, Boolean>>(
                new Callable() {
                    @Override public Object call() throws Exception {
                        boolean failed = false;

                        try {
                            client.message().remoteListen(null, new IgniteBiPredicate<UUID, Object>() {
                                @Override public boolean apply(UUID uuid, Object o) {
                                    if (o.equals("Test message."))
                                        recvLatch.countDown();

                                    return true;
                                }
                            });
                        }
                        catch (IgniteClientDisconnectedException e) {
                            failed = true;

                            checkAndWait(e);
                        }

                        assertTrue(failed);

                        return client.message().remoteListen(null, new IgniteBiPredicate<UUID, Object>() {
                            @Override public boolean apply(UUID uuid, Object o) {
                                if (o.equals("Test message."))
                                    recvLatch.countDown();

                                return true;
                            }
                        });
                    }
                },
                new C1<Object, Boolean>() {
                    @Override public Boolean apply(Object o) {
                        assertNotNull(o);

                        IgniteMessaging msg = client.message();

                        msg.send(null, "Test message.");

                        try {
                            assertTrue(recvLatch.await(2, SECONDS));
                        }
                        catch (InterruptedException ignored) {
                            fail("Message wasn't received.");
                        }

                        return true;
                    }
                }
            ),
            // Check executor.
            new T2<Callable, C1<Object, Boolean>>(
                new Callable() {
                    @Override public Object call() throws Exception {
                        boolean failed = false;

                        try {
                            client.executorService().submit(new Callable<Integer>() {
                                @Override public Integer call() throws Exception {
                                    return 42;
                                }
                            });
                        }
                        catch (IgniteClientDisconnectedException e) {
                            failed = true;

                            checkAndWait(e);
                        }

                        assertTrue(failed);

                        return client.executorService().submit(new Callable<Integer>() {
                            @Override public Integer call() throws Exception {
                                return 42;
                            }
                        });
                    }
                },
                new C1<Object, Boolean>() {
                    @Override public Boolean apply(Object o) {
                        assertNotNull(o);

                        Future<Integer> fut = (Future<Integer>)o;

                        try {
                            assertEquals(42, (int)fut.get());
                        }
                        catch (Exception ignored) {
                            fail("Failed submit task.");
                        }

                        return true;
                    }
                }
            )
        ));

        clientMode = false;
    }

    /**
     * @param client Client.
     * @param ops Operations closures.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void doTestIgniteOperationOnDisconnect(Ignite client, final List<T2<Callable, C1<Object, Boolean>>> ops)
        throws Exception {
        assertNotNull(client.cache(null));

        final TestTcpDiscoverySpi clientSpi = spi(client);

        Ignite srv = clientRouter(client);

        TestTcpDiscoverySpi srvSpi = spi(srv);

        final CountDownLatch disconnectLatch = new CountDownLatch(1);

        final CountDownLatch reconnectLatch = new CountDownLatch(1);

        log.info("Block reconnect.");

        clientSpi.writeLatch = new CountDownLatch(1);

        final List<IgniteInternalFuture> futs = new ArrayList<>();

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                    info("Disconnected: " + evt);

                    assertEquals(1, reconnectLatch.getCount());

                    for (T2<Callable, C1<Object, Boolean>> op : ops)
                        futs.add(GridTestUtils.runAsync(op.get1()));

                    disconnectLatch.countDown();
                }
                else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    info("Reconnected: " + evt);

                    reconnectLatch.countDown();
                }

                return true;
            }
        }, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);

        try {
            log.info("Fail client.");

            srvSpi.failNode(client.cluster().localNode().id(), null);

            waitReconnectEvent(disconnectLatch);

            assertEquals(ops.size(), futs.size());

            for (IgniteInternalFuture<?> fut : futs)
                assertNotDone(fut);

            U.sleep(2000);

            for (IgniteInternalFuture<?> fut : futs)
                assertNotDone(fut);

            log.info("Allow reconnect.");

            clientSpi.writeLatch.countDown();

            waitReconnectEvent(reconnectLatch);

            // Check operation after reconnect working.
            for (int i = 0; i < futs.size(); i++) {
                final int i0 = i;

                try {
                    final Object futRes = futs.get(i0).get(2, SECONDS);

                    assertTrue(GridTestUtils.runAsync(new Callable<Boolean>() {
                        @Override public Boolean call() throws Exception {
                            return ops.get(i0).get2().apply(futRes);
                        }
                    }).get(2, SECONDS));
                }
                catch (IgniteFutureTimeoutCheckedException e) {
                    e.printStackTrace();

                    fail("Operation timeout. Iteration: " + i + ".");
                }
            }
        }
        finally {
            clientSpi.writeLatch.countDown();

            for (IgniteInternalFuture fut : futs)
                fut.cancel();

            stopAllGrids();
        }
    }
}
