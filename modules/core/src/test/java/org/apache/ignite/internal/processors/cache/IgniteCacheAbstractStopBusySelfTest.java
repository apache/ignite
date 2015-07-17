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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 *
 */
public abstract class IgniteCacheAbstractStopBusySelfTest extends GridCommonAbstractTest {
    /** */
    public static final int CLN_GRD = 0;

    /** */
    public static final int SRV_GRD = 1;

    /** */
    public static final String CACHE_NAME = "StopTest";

    /** */
    public final TcpDiscoveryIpFinder finder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private AtomicBoolean suspended = new AtomicBoolean(false);

    /** */
    private CountDownLatch blocked;

    /** */
    protected AtomicReference<Class> bannedMsg = new AtomicReference<>();

    /**
     * @return Cache mode.
     */
    protected CacheMode cacheMode(){
        return CacheMode.PARTITIONED;
    }

    /**
     * @return Cache atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode(){
        return CacheAtomicityMode.TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cacheCfg = cacheConfiguration(CACHE_NAME);

        TestTpcCommunicationSpi commSpi = new TestTpcCommunicationSpi();

        commSpi.setLocalPort(GridTestUtils.getNextCommPort(getClass()));

        commSpi.setTcpNoDelay(true);

        if (gridName.endsWith(String.valueOf(CLN_GRD)))
            cfg.setClientMode(true);

        cacheCfg.setRebalanceMode(SYNC);

        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        cacheCfg.setBackups(1);

        cfg.setCommunicationSpi(commSpi);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(finder).setForceServerMode(true));

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        beforeTestsStarted();

        startGrid(SRV_GRD);

        startGrid(CLN_GRD);

        blocked = new CountDownLatch(1);

        for (int i = 0; i < 10; ++i) {
            if (clientNode().cluster().nodes().size() == 2)
                break;

            TimeUnit.MILLISECONDS.sleep(100L);
        }

        assertEquals(2, clientNode().cluster().nodes().size());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        suspended.set(false);

        bannedMsg.set(null);

        afterTestsStopped();

        stopGrid(SRV_GRD);

        stopGrid(CLN_GRD);

        List<Ignite> nodes = G.allGrids();

        assertTrue("Unexpected nodes: " + nodes, nodes.isEmpty());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        executeTest(new Callable<Integer>() {
            /** {@inheritDoc} */
            @Override public Integer call() throws Exception {
                info("Start operation.");

                Integer val = (Integer)clientCache().getAndPut(1, 999);

                info("Stop operation.");

                return val;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemove() throws Exception {
        executeTest(new Callable<Integer>() {
            /** {@inheritDoc} */
            @Override public Integer call() throws Exception {
                info("Start operation.");

                Integer val = (Integer)clientCache().getAndRemove(1);

                info("Stop operation.");

                return val;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAsync() throws Exception {
        executeTest(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                info("Start operation.");

                IgniteCache<Object, Object> cache = clientCache().withAsync();

                cache.getAndPut(1, 1);

                info("Stop operation.");

                return cache.future().get();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testGet() throws Exception {
        bannedMsg.set(GridNearGetRequest.class);

        executeTest(new Callable<Integer>() {
            /** {@inheritDoc} */
            @Override public Integer call() throws Exception {
                info("Start operation.");

                Integer put = (Integer) clientCache().get(1);

                info("Stop operation.");

                return put;
            }
        });
    }

    /**
     *
     * @param call Closure executing cache operation.
     * @throws Exception If failed.
     */
    private <T> void executeTest(Callable<T> call) throws Exception {
        suspended.set(true);

        IgniteInternalFuture<T> fut = GridTestUtils.runAsync(call);

        Thread stopThread = new Thread(new StopRunnable());

        blocked.await();

        stopThread.start();

        stopThread.join(10000L);

        suspended.set(false);

        assert !stopThread.isAlive();

        Exception e = null;

        try {
            fut.get();
        }
        catch (IgniteCheckedException gridException){
            e = gridException;
        }

        assertNotNull(e);

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        e.printStackTrace(pw);

        assertTrue(sw.toString().contains("node is stopping"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutBatch() throws Exception {
        assert !suspended.get();

        IgniteInternalFuture<Void> fut = GridTestUtils.runAsync(new Callable<Void>() {
            /** {@inheritDoc} */
            @Override public Void call() throws Exception {
                for (int i = 0; i < 1_000_000; i++)
                    clientCache().put(i, i);

                return null;
            }
        });

        Thread stopThread = new Thread(new StopRunnable());

        blocked.await();

        stopThread.start();

        stopThread.join(10000L);

        assert !stopThread.isAlive();

        Exception e = null;

        try {
            fut.get();
        }
        catch (IgniteCheckedException gridException){
            e = gridException;
        }

        assertNotNull(e);
    }

    /**
     * @return Client cache.
     */
    private Ignite clientNode() {
        return grid(CLN_GRD);
    }

    /**
     * @return Client cache.
     */
    private IgniteCache<Object, Object> clientCache() {
        return grid(CLN_GRD).cache(CACHE_NAME);
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    @SuppressWarnings("unchecked")
    private CacheConfiguration cacheConfiguration(@Nullable String cacheName) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(cacheMode());

        cfg.setAtomicityMode(atomicityMode());

        cfg.setNearConfiguration(null);

        cfg.setName(cacheName);

        return cfg;
    }

    /**
     *
     */
    private class TestTpcCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
            if (suspended.get()) {
                assert bannedMsg.get() != null;

                if (msg instanceof GridIoMessage
                    && ((GridIoMessage)msg).message().getClass().equals(bannedMsg.get())) {
                    blocked.countDown();

                    return;
                }
            }

            super.sendMessage(node, msg);
        }
    }

    /**
     *
     */
    private class StopRunnable implements Runnable {
        /** {@inheritDoc} */
        @Override public void run() {
            info("Stopping grid...");

            stopGrid(CLN_GRD, true);

            info("Grid stopped.");
        }
    }
}
