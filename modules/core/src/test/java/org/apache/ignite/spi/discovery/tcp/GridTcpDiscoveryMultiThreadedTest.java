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

package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.events.IgniteEventType.*;

/**
 * Test for {@link TcpDiscoverySpi}.
 */
public class GridTcpDiscoveryMultiThreadedTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 5;

    /** */
    private static final int CLIENT_GRID_CNT = 5;

    /** */
    private static final ThreadLocal<Boolean> clientFlagPerThread = new ThreadLocal<>();

    /** */
    private static volatile boolean clientFlagGlobal;

    /**
     * @return Client node flag.
     */
    private static boolean client() {
        Boolean client = clientFlagPerThread.get();

        return client != null ? client : clientFlagGlobal;
    }

    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     * @throws Exception If fails.
     */
    public GridTcpDiscoveryMultiThreadedTest() throws Exception {
        super(false);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"IfMayBeConditional"})
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (client()) {
            TcpClientDiscoverySpi spi = new TcpClientDiscoverySpi();

            spi.setIpFinder(ipFinder);

            cfg.setDiscoverySpi(spi);
        }
        else {
            TcpDiscoverySpi spi = new TcpDiscoverySpi();

            spi.setIpFinder(ipFinder);

            cfg.setDiscoverySpi(spi);
        }

        cfg.setCacheConfiguration();

        cfg.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);

        cfg.setIncludeProperties();

        cfg.setLocalHost("127.0.0.1");

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60 * 1000;
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testMultiThreaded() throws Exception {
        execute();
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testTopologyVersion() throws Exception {
        startGridsMultiThreaded(GRID_CNT);

        long prev = 0;

        for (Ignite g : G.allGrids()) {
            IgniteKernal kernal = (IgniteKernal)g;

            long ver = kernal.context().discovery().topologyVersion();

            info("Top ver: " + ver);

            if (prev == 0)
                prev = ver;
        }

        info("Test finished.");
    }

    /**
     * @throws Exception If failed.
     */
    private void execute() throws Exception {
        info("Test timeout: " + (getTestTimeout() / (60 * 1000)) + " min.");

        startGridsMultiThreaded(GRID_CNT);

        clientFlagGlobal = true;

        startGridsMultiThreaded(GRID_CNT, CLIENT_GRID_CNT);

        final AtomicBoolean done = new AtomicBoolean();

        final AtomicInteger clientIdx = new AtomicInteger(GRID_CNT);

        IgniteInternalFuture<?> fut1 = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    clientFlagPerThread.set(true);

                    int idx = clientIdx.getAndIncrement();

                    while (!done.get()) {
                        stopGrid(idx);
                        startGrid(idx);
                    }

                    return null;
                }
            },
            CLIENT_GRID_CNT
        );

        final BlockingQueue<Integer> srvIdx = new LinkedBlockingQueue<>();

        for (int i = 0; i < GRID_CNT; i++)
            srvIdx.add(i);

        IgniteInternalFuture<?> fut2 = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    clientFlagPerThread.set(false);

                    while (!done.get()) {
                        int idx = srvIdx.take();

                        stopGrid(idx);
                        startGrid(idx);

                        srvIdx.add(idx);
                    }

                    return null;
                }
            },
            GRID_CNT - 1
        );

        Thread.sleep(getTestTimeout() - 60 * 1000);

        done.set(true);

        fut1.get();
        fut2.get();
    }
}
