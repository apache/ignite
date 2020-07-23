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

package org.apache.ignite.spi.communication.tcp;

import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests for communication over discovery feature (inverse communication request).
 */
public class GridTotallyUnreachableClientTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache-0";

    /** */
    private static final int SRVS_NUM = 2;

    /** */
    private boolean forceClientToSrvConnections;

    /** */
    private int locPort;

    /** */
    private CacheConfiguration ccfg;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        forceClientToSrvConnections = false;
        locPort = TcpCommunicationSpi.DFLT_PORT;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureDetectionTimeout(8_000);

        cfg.setCommunicationSpi(
            new TcpCommunicationSpi()
                .setForceClientToServerConnections(forceClientToSrvConnections)
                .setLocalPort(locPort)
        );

        if (ccfg != null) {
            cfg.setCacheConfiguration(ccfg);

            ccfg = null;
        }

        return cfg;
    }

    /**
     * Test that you can't send anything from client to another client that has "-1" local port.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTotallyUnreachableClient() throws Exception {
        IgniteEx srv = startGrid(0);

        locPort = -1;
        IgniteEx client1 = startClientGrid(1);
        ClusterNode clientNode1 = client1.localNode();

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() ->
            srv.context().io().sendIoTest(clientNode1, new byte[10], false).get()
        );

        fut.get(30, TimeUnit.SECONDS);

        locPort = TcpCommunicationSpi.DFLT_PORT;

        IgniteEx client2 = startClientGrid(2);

        GridTestUtils.assertThrowsAnyCause(log, () -> {
            return GridTestUtils.runAsync(() ->
                client2.context().io().sendIoTest(clientNode1, new byte[10], false).get()
            ).get(30, TimeUnit.SECONDS);
        }, IgniteSpiException.class, "Cannot send");
    }

    /**
     * Executes cache test with "unreachable" client.
     *
     * @param forceClientToSrvConnections Flag for the client mode.
     * @throws Exception If failed.
     */
    private void executeCacheTestWithUnreachableClient(boolean forceClientToSrvConnections) throws Exception {
        LogListener lsnr = LogListener.matches("Failed to send message to remote node").atMost(0).build();

        for (int i = 0; i < SRVS_NUM; i++) {
            ccfg = cacheConfiguration(CACHE_NAME, ATOMIC);

            startGrid(i, (UnaryOperator<IgniteConfiguration>) cfg -> {
                ListeningTestLogger log = new ListeningTestLogger(false, cfg.getGridLogger());

                log.registerListener(lsnr);

                return cfg.setGridLogger(log);
            });
        }

        this.forceClientToSrvConnections = forceClientToSrvConnections;

        startClientGrid(SRVS_NUM);

        putAndCheckKey();

        assertTrue(lsnr.check());
    }

    /**
     * @param name Cache name.
     * @param atomicityMode Atomicity mode.
     * @return Cache configuration.
     */
    protected final CacheConfiguration cacheConfiguration(String name, CacheAtomicityMode atomicityMode) {
        CacheConfiguration ccfg = new CacheConfiguration(name);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(1);

        return ccfg;
    }

    /**
     * Puts a key to a server that is backup for the key and doesn't have an open communication connection to client.
     * This forces the server to establish a connection to "unreachable" client.
     */
    private void putAndCheckKey() {
        int key = 0;
        IgniteEx srv2 = grid(SRVS_NUM - 1);

        for (int i = 0; i < 1_000; i++) {
            if (srv2.affinity(CACHE_NAME).isBackup(srv2.localNode(), i)) {
                key = i;

                break;
            }
        }

        IgniteEx cl0 = grid(SRVS_NUM);

        IgniteCache<Object, Object> cache = cl0.cache(CACHE_NAME);

        cache.put(key, key);
        assertEquals(key, cache.get(key));
    }
}
