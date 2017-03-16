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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

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
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cacheCfg = cacheConfiguration(CACHE_NAME);

        TestTpcCommunicationSpi commSpi = new TestTpcCommunicationSpi();

        commSpi.setLocalPort(GridTestUtils.getNextCommPort(getClass()));

        commSpi.setTcpNoDelay(true);

        if (igniteInstanceName.endsWith(String.valueOf(CLN_GRD)))
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
        bannedMsg.set(GridNearSingleGetRequest.class);

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
     * @throws Exception If failed.
     */
    public void testGetAll() throws Exception {
        bannedMsg.set(GridNearGetRequest.class);

        executeTest(new Callable<Integer>() {
            /** {@inheritDoc} */
            @Override public Integer call() throws Exception {
                info("Start operation.");

                Set<Integer> keys = F.asSet(1, 2, 3);

                clientCache().getAll(keys);

                info("Stop operation.");

                return null;
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
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            if (suspended.get()) {
                assert bannedMsg.get() != null;

                if (msg instanceof GridIoMessage
                    && ((GridIoMessage)msg).message().getClass().equals(bannedMsg.get())) {
                    blocked.countDown();

                    return;
                }
            }

            super.sendMessage(node, msg, ackClosure);
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