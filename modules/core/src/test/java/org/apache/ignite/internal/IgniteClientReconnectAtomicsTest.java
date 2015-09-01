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

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.testframework.GridTestUtils;

/**
 *
 */
public class IgniteClientReconnectAtomicsTest extends IgniteClientReconnectAbstractTest {
    /** {@inheritDoc} */
    @Override protected int serverCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected int clientCount() {
        return 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicSeqReconnect() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

        IgniteAtomicSequence clientAtomicSeq = client.atomicSequence("atomicSeq", 0, true);

        assertEquals(1L, clientAtomicSeq.incrementAndGet());

        final IgniteAtomicSequence srvAtomicSeq = srv.atomicSequence("atomicSeq", 0, false);

        assertEquals(1001L, srvAtomicSeq.incrementAndGet());

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                assertEquals(1002L, srvAtomicSeq.incrementAndGet());
            }
        });

        assertEquals(2L, clientAtomicSeq.incrementAndGet());

        assertEquals(1003L, srvAtomicSeq.incrementAndGet());

        assertEquals(3L, clientAtomicSeq.incrementAndGet());

        clientAtomicSeq.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicSeqReconnectRemoved() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

        final IgniteAtomicSequence clientAtomicSeq = client.atomicSequence("atomicSeqRmv", 0, true);

        clientAtomicSeq.batchSize(1);

        assertEquals(1L, clientAtomicSeq.incrementAndGet());

        final IgniteAtomicSequence srvAtomicSeq = srv.atomicSequence("atomicSeqRmv", 0, false);

        srvAtomicSeq.batchSize(1);

        assertEquals(1001L, srvAtomicSeq.incrementAndGet());

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                srvAtomicSeq.close();

                assert srvAtomicSeq.removed();
            }
        });

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 0; i < 2000; i++)
                    clientAtomicSeq.incrementAndGet();

                return null;
            }
        }, IllegalStateException.class, null);

        IgniteAtomicSequence newClientAtomicSeq = client.atomicSequence("atomicSeqRmv", 0, true);

        assertEquals(0, newClientAtomicSeq.get());

        assertEquals(1, newClientAtomicSeq.incrementAndGet());

        newClientAtomicSeq.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicSeqReconnectInProgress() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

        BlockTpcCommunicationSpi commSpi = commSpi(srv);

        final IgniteAtomicSequence clientAtomicSeq = client.atomicSequence("atomicSeqInProg", 0, true);

        clientAtomicSeq.batchSize(1);

        final IgniteAtomicSequence srvAtomicSeq = srv.atomicSequence("atomicSeqInProg", 0, false);

        srvAtomicSeq.batchSize(1);

        commSpi.blockMessage(GridNearLockResponse.class);

        final IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 0; i < 3000; i++) {
                    try {
                        clientAtomicSeq.incrementAndGet();
                    }
                    catch (IgniteClientDisconnectedException e) {
                        checkAndWait(e);

                        return true;
                    }
                }

                return false;
            }
        });

        // Check that client waiting operation.
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fut.get(200);
            }
        }, IgniteFutureTimeoutCheckedException.class, null);

        assertNotDone(fut);

        commSpi.unblockMessage();

        reconnectClientNode(client, srv, null);

        assertTrue((Boolean)fut.get(2, TimeUnit.SECONDS));

        // Check that after reconnect working.
        assert clientAtomicSeq.incrementAndGet() >= 0;
        assert srvAtomicSeq.incrementAndGet() >= 0;

        clientAtomicSeq.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicReferenceReconnect() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

        IgniteAtomicReference<String> clientAtomicRef = client.atomicReference("atomicRef", "1st value", true);

        assertEquals("1st value", clientAtomicRef.get());
        assertTrue(clientAtomicRef.compareAndSet("1st value", "2st value"));
        assertEquals("2st value", clientAtomicRef.get());

        final IgniteAtomicReference<String> srvAtomicRef = srv.atomicReference("atomicRef", "1st value", false);

        assertEquals("2st value", srvAtomicRef.get());
        assertTrue(srvAtomicRef.compareAndSet("2st value", "3st value"));
        assertEquals("3st value", srvAtomicRef.get());

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                assertEquals("3st value", srvAtomicRef.get());
                assertTrue(srvAtomicRef.compareAndSet("3st value", "4st value"));
                assertEquals("4st value", srvAtomicRef.get());
            }
        });

        assertEquals("4st value", clientAtomicRef.get());
        assertTrue(clientAtomicRef.compareAndSet("4st value", "5st value"));
        assertEquals("5st value", clientAtomicRef.get());

        assertEquals("5st value", srvAtomicRef.get());
        assertTrue(srvAtomicRef.compareAndSet("5st value", "6st value"));
        assertEquals("6st value", srvAtomicRef.get());

        srvAtomicRef.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicReferenceReconnectRemoved() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

        final IgniteAtomicReference<String> clientAtomicRef =
            client.atomicReference("atomicRefRemoved", "1st value", true);

        assertEquals("1st value", clientAtomicRef.get());
        assertTrue(clientAtomicRef.compareAndSet("1st value", "2st value"));
        assertEquals("2st value", clientAtomicRef.get());

        final IgniteAtomicReference<String> srvAtomicRef = srv.atomicReference("atomicRefRemoved", "1st value", false);

        assertEquals("2st value", srvAtomicRef.get());
        assertTrue(srvAtomicRef.compareAndSet("2st value", "3st value"));
        assertEquals("3st value", srvAtomicRef.get());

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                srvAtomicRef.close();
            }
        });

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                clientAtomicRef.compareAndSet("3st value", "4st value");

                return null;
            }
        }, IllegalStateException.class, null);

        IgniteAtomicReference<String> newClientAtomicRef =
            client.atomicReference("atomicRefRemoved", "1st value", true);

        IgniteAtomicReference<String> newSrvAtomicRef = srv.atomicReference("atomicRefRemoved", "1st value", false);

        assertEquals("1st value", newClientAtomicRef.get());
        assertTrue(newClientAtomicRef.compareAndSet("1st value", "2st value"));
        assertEquals("2st value", newClientAtomicRef.get());

        assertEquals("2st value", newSrvAtomicRef.get());
        assertTrue(newSrvAtomicRef.compareAndSet("2st value", "3st value"));
        assertEquals("3st value", newSrvAtomicRef.get());

        newClientAtomicRef.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicReferenceReconnectInProgress() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

        final IgniteAtomicReference<String> clientAtomicRef =
            client.atomicReference("atomicRefInProg", "1st value", true);

        assertEquals("1st value", clientAtomicRef.get());
        assertTrue(clientAtomicRef.compareAndSet("1st value", "2st value"));
        assertEquals("2st value", clientAtomicRef.get());

        IgniteAtomicReference<String> srvAtomicRef = srv.atomicReference("atomicRefInProg", "1st value", false);

        assertEquals("2st value", srvAtomicRef.get());
        assertTrue(srvAtomicRef.compareAndSet("2st value", "3st value"));
        assertEquals("3st value", srvAtomicRef.get());

        BlockTpcCommunicationSpi servCommSpi = commSpi(srv);

        servCommSpi.blockMessage(GridNearLockResponse.class);

        final IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try {
                    clientAtomicRef.compareAndSet("3st value", "4st value");
                }
                catch (IgniteClientDisconnectedException e) {
                    checkAndWait(e);

                    return true;
                }

                return false;
            }
        });

        // Check that client waiting operation.
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fut.get(200);
            }
        }, IgniteFutureTimeoutCheckedException.class, null);

        assertNotDone(fut);

        servCommSpi.unblockMessage();

        reconnectClientNode(client, srv, null);

        assertTrue((Boolean)fut.get(2, TimeUnit.SECONDS));

        // Check that after reconnect working.
        assertEquals("3st value", clientAtomicRef.get());
        assertTrue(clientAtomicRef.compareAndSet("3st value", "4st value"));
        assertEquals("4st value", clientAtomicRef.get());

        assertEquals("4st value", srvAtomicRef.get());
        assertTrue(srvAtomicRef.compareAndSet("4st value", "5st value"));
        assertEquals("5st value", srvAtomicRef.get());

        srvAtomicRef.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicStampedReconnect() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

        IgniteAtomicStamped clientAtomicStamped = client.atomicStamped("atomicStamped", 0, 0, true);

        assertEquals(true, clientAtomicStamped.compareAndSet(0, 1, 0, 1));
        assertEquals(1, clientAtomicStamped.value());
        assertEquals(1, clientAtomicStamped.stamp());

        final IgniteAtomicStamped srvAtomicStamped = srv.atomicStamped("atomicStamped", 0, 0, false);

        assertEquals(true, srvAtomicStamped.compareAndSet(1, 2, 1, 2));
        assertEquals(2, srvAtomicStamped.value());
        assertEquals(2, srvAtomicStamped.stamp());

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                assertEquals(true, srvAtomicStamped.compareAndSet(2, 3, 2, 3));
                assertEquals(3, srvAtomicStamped.value());
                assertEquals(3, srvAtomicStamped.stamp());
            }
        });

        assertEquals(true, clientAtomicStamped.compareAndSet(3, 4, 3, 4));
        assertEquals(4, clientAtomicStamped.value());
        assertEquals(4, clientAtomicStamped.stamp());

        assertEquals(true, srvAtomicStamped.compareAndSet(4, 5, 4, 5));
        assertEquals(5, srvAtomicStamped.value());
        assertEquals(5, srvAtomicStamped.stamp());

        srvAtomicStamped.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicStampedReconnectRemoved() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

        final IgniteAtomicStamped clientAtomicStamped = client.atomicStamped("atomicStampedRemoved", 0, 0, true);

        assertEquals(true, clientAtomicStamped.compareAndSet(0, 1, 0, 1));
        assertEquals(1, clientAtomicStamped.value());
        assertEquals(1, clientAtomicStamped.stamp());

        final IgniteAtomicStamped srvAtomicStamped = srv.atomicStamped("atomicStampedRemoved", 0, 0, false);

        assertEquals(true, srvAtomicStamped.compareAndSet(1, 2, 1, 2));
        assertEquals(2, srvAtomicStamped.value());
        assertEquals(2, srvAtomicStamped.stamp());

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                srvAtomicStamped.close();
            }
        });

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                clientAtomicStamped.compareAndSet(2, 3, 2, 3);

                return null;
            }
        }, IllegalStateException.class, null);

        IgniteAtomicStamped newClientAtomicStamped = client.atomicStamped("atomicStampedRemoved", 0, 0, true);

        assertEquals(true, newClientAtomicStamped.compareAndSet(0, 1, 0, 1));
        assertEquals(1, newClientAtomicStamped.value());
        assertEquals(1, newClientAtomicStamped.stamp());

        IgniteAtomicStamped newSrvAtomicStamped = srv.atomicStamped("atomicStampedRemoved", 0, 0, false);

        assertEquals(true, newSrvAtomicStamped.compareAndSet(1, 2, 1, 2));
        assertEquals(2, newSrvAtomicStamped.value());
        assertEquals(2, newSrvAtomicStamped.stamp());

        newClientAtomicStamped.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicStampedReconnectInProgress() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

        final IgniteAtomicStamped clientAtomicStamped = client.atomicStamped("atomicStampedInProgress", 0, 0, true);

        assertEquals(true, clientAtomicStamped.compareAndSet(0, 1, 0, 1));
        assertEquals(1, clientAtomicStamped.value());
        assertEquals(1, clientAtomicStamped.stamp());

        IgniteAtomicStamped srvAtomicStamped = srv.atomicStamped("atomicStampedInProgress", 0, 0, false);

        assertEquals(true, srvAtomicStamped.compareAndSet(1, 2, 1, 2));
        assertEquals(2, srvAtomicStamped.value());
        assertEquals(2, srvAtomicStamped.stamp());

        BlockTpcCommunicationSpi servCommSpi = commSpi(srv);

        servCommSpi.blockMessage(GridNearLockResponse.class);

        final IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try {
                    clientAtomicStamped.compareAndSet(2, 3, 2, 3);
                }
                catch (IgniteClientDisconnectedException e) {
                    checkAndWait(e);

                    return true;
                }

                return false;
            }
        });

        // Check that client waiting operation.
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fut.get(200);
            }
        }, IgniteFutureTimeoutCheckedException.class, null);

        assertNotDone(fut);

        servCommSpi.unblockMessage();

        reconnectClientNode(client, srv, null);

        assertTrue((Boolean)fut.get(2, TimeUnit.SECONDS));

        // Check that after reconnect working.
        assertEquals(true, clientAtomicStamped.compareAndSet(2, 3, 2, 3));
        assertEquals(3, clientAtomicStamped.value());
        assertEquals(3, clientAtomicStamped.stamp());

        assertEquals(true, srvAtomicStamped.compareAndSet(3, 4, 3, 4));
        assertEquals(4, srvAtomicStamped.value());
        assertEquals(4, srvAtomicStamped.stamp());

        srvAtomicStamped.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicLongReconnect() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

        IgniteAtomicLong clientAtomicLong = client.atomicLong("atomicLong", 0, true);

        assertEquals(0L, clientAtomicLong.getAndAdd(1));

        final IgniteAtomicLong srvAtomicLong = srv.atomicLong("atomicLong", 0, false);

        assertEquals(1L, srvAtomicLong.getAndAdd(1));

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                assertEquals(2L, srvAtomicLong.getAndAdd(1));
            }
        });

        assertEquals(3L, clientAtomicLong.getAndAdd(1));

        assertEquals(4L, srvAtomicLong.getAndAdd(1));

        assertEquals(5L, clientAtomicLong.getAndAdd(1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicLongReconnectRemoved() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

        final IgniteAtomicLong clientAtomicLong = client.atomicLong("atomicLongRmv", 0, true);

        assertEquals(0L, clientAtomicLong.getAndAdd(1));

        final IgniteAtomicLong srvAtomicLong = srv.atomicLong("atomicLongRmv", 0, false);

        assertEquals(1L, srvAtomicLong.getAndAdd(1));

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                srvAtomicLong.close();
            }
        });

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                clientAtomicLong.getAndAdd(1);

                return null;
            }
        }, IllegalStateException.class, null);

        IgniteAtomicLong newClientAtomicLong = client.atomicLong("atomicLongRmv", 0, true);

        assertEquals(0L, newClientAtomicLong.getAndAdd(1));

        IgniteAtomicLong newSrvAtomicLong = srv.atomicLong("atomicLongRmv", 0, false);

        assertEquals(1L, newSrvAtomicLong.getAndAdd(1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicLongReconnectInProgress() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

        BlockTpcCommunicationSpi commSpi = commSpi(srv);

        final IgniteAtomicLong clientAtomicLong = client.atomicLong("atomicLongInProggress", 0, true);

        final IgniteAtomicLong srvAtomicLong = srv.atomicLong("atomicLongInProggress", 0, false);

        commSpi.blockMessage(GridNearLockResponse.class);

        final IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try {
                    clientAtomicLong.getAndAdd(1);
                }
                catch (IgniteClientDisconnectedException e) {
                    checkAndWait(e);

                    return true;
                }

                return false;
            }
        });

        // Check that client waiting operation.
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fut.get(200);
            }
        }, IgniteFutureTimeoutCheckedException.class, null);

        assertNotDone(fut);

        commSpi.unblockMessage();

        reconnectClientNode(client, srv, null);

        assertTrue((Boolean)fut.get(2, TimeUnit.SECONDS));

        // Check that after reconnect working.
        assertEquals(1, clientAtomicLong.addAndGet(1));
        assertEquals(2, srvAtomicLong.addAndGet(1));

        clientAtomicLong.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLatchReconnect() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

        IgniteCountDownLatch clientLatch = client.countDownLatch("latch1", 3, false, true);

        assertEquals(3, clientLatch.count());

        final IgniteCountDownLatch srvLatch = srv.countDownLatch("latch1", 3, false, false);

        assertEquals(3, srvLatch.count());

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                srvLatch.countDown();
            }
        });

        assertEquals(2, srvLatch.count());
        assertEquals(2, clientLatch.count());

        srvLatch.countDown();

        assertEquals(1, srvLatch.count());
        assertEquals(1, clientLatch.count());

        clientLatch.countDown();

        assertEquals(0, srvLatch.count());
        assertEquals(0, clientLatch.count());

        assertTrue(srvLatch.await(1000));
        assertTrue(clientLatch.await(1000));
    }
}