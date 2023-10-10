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

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class IgniteClientReconnectCollectionsTest extends IgniteClientReconnectAbstractTest {
    /** */
    private static final CollectionConfiguration TX_CFGS = new CollectionConfiguration();

    /** */
    private static final CollectionConfiguration ATOMIC_CONF = new CollectionConfiguration();

    static {
        TX_CFGS.setCacheMode(PARTITIONED);
        TX_CFGS.setAtomicityMode(TRANSACTIONAL);

        ATOMIC_CONF.setCacheMode(PARTITIONED);
        ATOMIC_CONF.setAtomicityMode(ATOMIC);
    }

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
    @Test
    public void testCollectionsReconnectClusterRestart() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        final IgniteQueue<Object> queue = client.queue("q", 0, TX_CFGS);
        final IgniteSet<Object> set = client.set("s", TX_CFGS);

        Ignite srv = grid(0);

        reconnectServersRestart(log, client, Collections.singleton(srv), new Callable<Collection<Ignite>>() {
            @Override public Collection<Ignite> call() throws Exception {
                return Collections.singleton((Ignite)startGrid(0));
            }
        });

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                queue.add(1);

                return null;
            }
        }, IllegalStateException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                set.add(1);

                return null;
            }
        }, IllegalStateException.class, null);

        try (IgniteQueue<Object> queue2 = client.queue("q", 0, TX_CFGS)) {
            queue2.add(1);

            assert queue2.size() == 1 : queue2.size();
        }

        try (IgniteSet<Object> set2 = client.set("s", TX_CFGS)) {
            set2.add(1);

            assert set2.size() == 1 : set2.size();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueueReconnect() throws Exception {
        queueReconnect(TX_CFGS);

        queueReconnect(ATOMIC_CONF);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueueReconnectRemoved() throws Exception {
        queueReconnectRemoved(TX_CFGS);

        queueReconnectRemoved(ATOMIC_CONF);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueueReconnectInProgress() throws Exception {
        queueReconnectInProgress(TX_CFGS);

        queueReconnectInProgress(ATOMIC_CONF);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetReconnect() throws Exception {
        setReconnect(TX_CFGS);

        setReconnect(ATOMIC_CONF);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetReconnectRemoved() throws Exception {
        setReconnectRemove(TX_CFGS);

        setReconnectRemove(ATOMIC_CONF);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetReconnectInProgress() throws Exception {
        setReconnectInProgress(TX_CFGS);

        setReconnectInProgress(ATOMIC_CONF);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testServerReconnect() throws Exception {
        serverNodeReconnect(TX_CFGS);

        serverNodeReconnect(ATOMIC_CONF);
    }

    /**
     * @param colCfg Collection configuration.
     * @throws Exception If failed.
     */
    private void serverNodeReconnect(CollectionConfiguration colCfg) throws Exception {
        final Ignite client = grid(serverCount());

        final Ignite srv = ignite(0);

        assertNotNull(srv.queue("q", 0, colCfg));
        assertNotNull(srv.set("s", colCfg));

        reconnectClientNode(client, srv, null);

        IgniteQueue<Object> q = client.queue("q", 0, null);

        assertNotNull(q);
    }

    /**
     * @param colCfg Collection configuration.
     * @throws Exception If failed.
     */
    private void setReconnect(CollectionConfiguration colCfg) throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = ignite(0);

        final String setName = "set-" + colCfg.getAtomicityMode();

        IgniteSet<String> clientSet = client.set(setName, colCfg);

        final IgniteSet<String> srvSet = srv.set(setName, null);

        assertTrue(clientSet.add("1"));

        assertFalse(srvSet.add("1"));

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                assertTrue(srvSet.add("2"));
            }
        });

        assertFalse(clientSet.add("2"));

        assertTrue(clientSet.remove("2"));

        assertFalse(srvSet.contains("2"));
    }

    /**
     * @param colCfg Collection configuration.
     * @throws Exception If failed.
     */
    private void setReconnectRemove(CollectionConfiguration colCfg) throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        final Ignite srv = ignite(0);

        final String setName = "set-rm-" + colCfg.getAtomicityMode();

        final IgniteSet<String> clientSet = client.set(setName, colCfg);

        final IgniteSet<String> srvSet = srv.set(setName, null);

        assertTrue(clientSet.add("1"));

        assertFalse(srvSet.add("1"));

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                srvSet.close();
            }
        });

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                clientSet.add("fail");

                return null;
            }
        }, IllegalStateException.class, null);

        IgniteSet<String> newClientSet = client.set(setName, colCfg);

        IgniteSet<String> newSrvSet = srv.set(setName, null);

        assertTrue(newClientSet.add("1"));

        assertFalse(newSrvSet.add("1"));

        newSrvSet.close();
    }

    /**
     * @param colCfg Collection configuration.
     * @throws Exception If failed.
     */
    private void setReconnectInProgress(final CollectionConfiguration colCfg) throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        final Ignite srv = ignite(0);

        final String setName = "set-in-progress-" + colCfg.getAtomicityMode();

        final IgniteSet<String> clientSet = client.set(setName, colCfg);

        final IgniteSet<String> srvSet = srv.set(setName, null);

        assertTrue(clientSet.add("1"));

        assertFalse(srvSet.add("1"));

        BlockTcpCommunicationSpi commSpi = commSpi(srv);

        if (colCfg.getAtomicityMode() == ATOMIC)
            commSpi.blockMessage(GridNearAtomicUpdateResponse.class);
        else
            commSpi.blockMessage(GridNearTxPrepareResponse.class);

        final IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try {
                    for (int i = 0; i < 100; i++)
                        clientSet.add("2");
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

        assertTrue(clientSet.add("3"));

        assertFalse(srvSet.add("3"));

        srvSet.close();
    }

    /**
     * @param colCfg Collection configuration.
     * @throws Exception If failed.
     */
    private void queueReconnect(CollectionConfiguration colCfg) throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = ignite(0);

        final String setName = "queue-" + colCfg.getAtomicityMode();

        IgniteQueue<String> clientQueue = client.queue(setName, 10, colCfg);

        final IgniteQueue<String> srvQueue = srv.queue(setName, 10, null);

        assertTrue(clientQueue.offer("1"));

        assertTrue(srvQueue.contains("1"));

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                assertTrue(srvQueue.add("2"));
            }
        });

        assertTrue(clientQueue.contains("2"));

        assertEquals("1", clientQueue.poll());
    }

    /**
     * @param colCfg Collection configuration.
     * @throws Exception If failed.
     */
    private void queueReconnectRemoved(CollectionConfiguration colCfg) throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = ignite(0);

        final String setName = "queue-rmv" + colCfg.getAtomicityMode();

        final IgniteQueue<String> clientQueue = client.queue(setName, 10, colCfg);

        final IgniteQueue<String> srvQueue = srv.queue(setName, 10, null);

        assertTrue(clientQueue.add("1"));

        assertTrue(srvQueue.add("2"));

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                srvQueue.close();
            }
        });

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                clientQueue.add("fail");

                return null;
            }
        }, IllegalStateException.class, null);

        IgniteQueue<String> newClientQueue = client.queue(setName, 10, colCfg);

        IgniteQueue<String> newSrvQueue = srv.queue(setName, 10, null);

        assertTrue(newClientQueue.add("1"));

        assertTrue(newSrvQueue.add("2"));
    }

    /**
     * @param colCfg Collection configuration.
     * @throws Exception If failed.
     */
    private void queueReconnectInProgress(final CollectionConfiguration colCfg) throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = ignite(0);

        final String setName = "queue-rmv" + colCfg.getAtomicityMode();

        final IgniteQueue<String> clientQueue = client.queue(setName, 10, colCfg);

        final IgniteQueue<String> srvQueue = srv.queue(setName, 10, null);

        assertTrue(clientQueue.offer("1"));

        assertTrue(srvQueue.contains("1"));

        BlockTcpCommunicationSpi commSpi = commSpi(srv);

        if (colCfg.getAtomicityMode() == ATOMIC)
            commSpi.blockMessage(GridNearAtomicUpdateResponse.class);
        else
            commSpi.blockMessage(GridNearTxPrepareResponse.class);

        final IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try {
                    clientQueue.add("2");
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

        assertTrue("Future was not failed. Atomic mode: " + colCfg.getAtomicityMode() + ".", (Boolean)fut.get());

        assertTrue(clientQueue.add("3"));

        assertEquals("1", clientQueue.poll());
    }
}
