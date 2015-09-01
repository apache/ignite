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
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerResponse;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class IgniteClientReconnectStreamerTest extends IgniteClientReconnectAbstractTest {
    /** */
    public static final String CACHE_NAME = "streamer";

    /** {@inheritDoc} */
    @Override protected int serverCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected int clientCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<Integer, Integer>(CACHE_NAME)
            .setAtomicityMode(ATOMIC)
            .setCacheMode(PARTITIONED);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStreamerReconnect() throws Exception {
        final Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

        final IgniteCache<Object, Object> srvCache = srv.cache(CACHE_NAME);

        IgniteDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME);

        for (int i = 0; i < 50; i++)
            streamer.addData(i, i);

        streamer.flush();

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return srvCache.localSize() == 50;
            }
        }, 2000L);

        assertEquals(50, srvCache.localSize());

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                try {
                    client.dataStreamer(CACHE_NAME);

                    fail();
                }
                catch (IgniteClientDisconnectedException e) {
                    assertNotNull(e.reconnectFuture());
                }
            }
        });

        checkStreamerClosed(streamer);

        streamer = client.dataStreamer(CACHE_NAME);

        for (int i = 50; i < 100; i++)
            streamer.addData(i, i);

        streamer.flush();

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return srvCache.localSize() == 100;
            }
        }, 2000L);

        assertEquals(100, srvCache.localSize());

        streamer.close();

        streamer.future().get(2, TimeUnit.SECONDS);

        srvCache.removeAll();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStreamerReconnectInProgress() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

        final IgniteCache<Object, Object> srvCache = srv.cache(CACHE_NAME);

        final IgniteDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME);

        BlockTpcCommunicationSpi commSpi = commSpi(srv);

        commSpi.blockMessage(DataStreamerResponse.class);

        final IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try {
                    for (int i = 0; i < 50; i++)
                        streamer.addData(i, i);

                    streamer.flush();
                }
                catch (CacheException e) {
                    checkAndWait(e);

                    return true;
                }
                finally {
                    streamer.close();
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

        checkStreamerClosed(streamer);

        IgniteDataStreamer<Integer, Integer> streamer2 = client.dataStreamer(CACHE_NAME);

        for (int i = 0; i < 50; i++)
            streamer2.addData(i, i);

        streamer2.close();

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return srvCache.localSize() == 50;
            }
        }, 2000L);

        assertEquals(50, srvCache.localSize());
    }

    /**
     * @param streamer Streamer.
     */
    private void checkStreamerClosed(IgniteDataStreamer<Integer, Integer> streamer) {
        try {
            streamer.addData(100, 100);

            fail();
        }
        catch (CacheException e) {
            checkAndWait(e);
        }

        try {
            streamer.flush();

            fail();
        }
        catch (CacheException e) {
            checkAndWait(e);
        }

        try {
            streamer.future().get();

            fail();
        }
        catch (CacheException e) {
            checkAndWait(e);
        }

        streamer.tryFlush();

        streamer.close();
    }
}