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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.testframework.*;

import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;

/**
 *
 */
public class IgniteClientReconnectCollectionsTest extends IgniteClientReconnectAbstractTest {
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
    public void testQueueReconnect() throws Exception {
        CollectionConfiguration colCfg = new CollectionConfiguration();

        colCfg.setCacheMode(PARTITIONED);
        colCfg.setAtomicityMode(TRANSACTIONAL);

        queueReconnect(colCfg);

        colCfg = new CollectionConfiguration();

        colCfg.setCacheMode(PARTITIONED);
        colCfg.setAtomicityMode(ATOMIC);

        queueReconnect(colCfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueueReconnectRemoved() throws Exception {
        CollectionConfiguration colCfg = new CollectionConfiguration();

        colCfg.setCacheMode(PARTITIONED);
        colCfg.setAtomicityMode(TRANSACTIONAL);

        queueReconnectRemoved(colCfg);

        colCfg = new CollectionConfiguration();

        colCfg.setCacheMode(PARTITIONED);
        colCfg.setAtomicityMode(ATOMIC);

        queueReconnectRemoved(colCfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueueReconnectInProgress() throws Exception {
        CollectionConfiguration colCfg = new CollectionConfiguration();

        colCfg.setCacheMode(PARTITIONED);
        colCfg.setAtomicityMode(TRANSACTIONAL);

        queueReconnectInProgress(colCfg);

        colCfg = new CollectionConfiguration();

        colCfg.setCacheMode(PARTITIONED);
        colCfg.setAtomicityMode(ATOMIC);

        queueReconnectInProgress(colCfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetReconnect() throws Exception {
        CollectionConfiguration colCfg = new CollectionConfiguration();

        colCfg.setCacheMode(PARTITIONED);
        colCfg.setAtomicityMode(TRANSACTIONAL);

        setReconnect(colCfg);

        colCfg = new CollectionConfiguration();

        colCfg.setCacheMode(PARTITIONED);
        colCfg.setAtomicityMode(ATOMIC);

        setReconnect(colCfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetReconnectRemoved() throws Exception {
        CollectionConfiguration colCfg = new CollectionConfiguration();

        colCfg.setCacheMode(PARTITIONED);
        colCfg.setAtomicityMode(ATOMIC);

        setReconnectRemove(colCfg);

        colCfg = new CollectionConfiguration();

        colCfg.setCacheMode(PARTITIONED);
        colCfg.setAtomicityMode(TRANSACTIONAL);

        setReconnectRemove(colCfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetReconnectInProgress() throws Exception {
        CollectionConfiguration colCfg = new CollectionConfiguration();

        colCfg.setCacheMode(PARTITIONED);
        colCfg.setAtomicityMode(ATOMIC);

        setReconnectInProgress(colCfg);

        colCfg = new CollectionConfiguration();

        colCfg.setCacheMode(PARTITIONED);
        colCfg.setAtomicityMode(TRANSACTIONAL);

        setReconnectInProgress(colCfg);
    }

    /**
     * @param colCfg Collection configuration.
     * @throws Exception If failed.
     */
    private void setReconnect(CollectionConfiguration colCfg) throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

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

        final Ignite srv = clientRouter(client);

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

        final Ignite srv = clientRouter(client);

        final String setName = "set-in-progress-" + colCfg.getAtomicityMode();

        final IgniteSet<String> clientSet = client.set(setName, colCfg);

        final IgniteSet<String> srvSet = srv.set(setName, null);

        assertTrue(clientSet.add("1"));

        assertFalse(srvSet.add("1"));

        BlockTpcCommunicationSpi commSpi = commSpi(srv);

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

        Ignite srv = clientRouter(client);

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

        Ignite srv = clientRouter(client);

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

        Ignite srv = clientRouter(client);

        final String setName = "queue-rmv" + colCfg.getAtomicityMode();

        final IgniteQueue<String> clientQueue = client.queue(setName, 10, colCfg);

        final IgniteQueue<String> srvQueue = srv.queue(setName, 10, null);

        assertTrue(clientQueue.offer("1"));

        assertTrue(srvQueue.contains("1"));

        BlockTpcCommunicationSpi commSpi = commSpi(srv);

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
