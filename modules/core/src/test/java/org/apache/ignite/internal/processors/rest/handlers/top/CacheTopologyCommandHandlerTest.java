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

package org.apache.ignite.internal.processors.rest.handlers.top;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.request.GridRestTopologyRequest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class CacheTopologyCommandHandlerTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        // Discovery config.
        TcpDiscoverySpi disco = new TcpDiscoverySpi()
            .setIpFinder(new TcpDiscoveryVmIpFinder(true))
            .setJoinTimeout(5000);

        // Cache config.
        CacheConfiguration ccfg = new CacheConfiguration("cache*")
            .setCacheMode(CacheMode.LOCAL)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC);

        ConnectorConfiguration clnCfg = new ConnectorConfiguration()
            .setHost("127.0.0.1");

        return super.getConfiguration()
            .setLocalHost("127.0.0.1")
            .setConnectorConfiguration(clnCfg)
            .setDiscoverySpi(disco)
            .setCacheConfiguration(ccfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTopologyCommandOnDynamicCacheCreateDestroy() throws Exception {
        GridRestTopologyRequest req = new GridRestTopologyRequest();
        req.command(GridRestCommand.TOPOLOGY);

        topologyCommandOnDynamicCacheCreateDestroy(startGrid(), req);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeCommandOnDynamicCacheCreateDestroy1() throws Exception {
        Ignite node = startGrid();

        GridRestTopologyRequest req = new GridRestTopologyRequest();
        req.command(GridRestCommand.NODE);
        req.nodeId(node.cluster().localNode().id());

        topologyCommandOnDynamicCacheCreateDestroy(node, req);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeCommandOnDynamicCacheCreateDestroy2() throws Exception {
        Ignite node = startGrid();

        GridRestTopologyRequest req = new GridRestTopologyRequest();
        req.command(GridRestCommand.NODE);
        req.nodeIp("127.0.0.1");

        topologyCommandOnDynamicCacheCreateDestroy(node, req);
    }

    /**
     * @param node Ignite node.
     * @param req Rest request.
     * @throws Exception If failed.
     */
    private void topologyCommandOnDynamicCacheCreateDestroy(final Ignite node, GridRestTopologyRequest req) throws Exception {
        GridTopologyCommandHandler hnd = new GridTopologyCommandHandler(((IgniteKernal)node).context());

        final AtomicReference<Exception> ex = new AtomicReference<>();

        final long deadline = System.currentTimeMillis() + 5000;

        final AtomicInteger cntr = new AtomicInteger();

        IgniteInternalFuture fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                int startIdx = cntr.getAndAdd(4);
                int idx = 0;
                boolean start = true;

                while (System.currentTimeMillis() < deadline) {
                    try {
                        if (start)
                            node.createCache("cache" + (startIdx + idx));
                        else
                            node.destroyCache("cache" + (startIdx + idx));

                        if ((idx = (idx + 1) % 4) == 0)
                            start = !start;
                    }
                    catch (Exception e) {
                        if (ex.get() != null || !ex.compareAndSet(null, e))
                            ex.get().addSuppressed(e);

                        break;
                    }
                }
            }
        }, 4, "cache-start-destroy");

        while (!fut.isDone())
            hnd.handleAsync(req).get();

        if (ex.get() != null)
            throw ex.get();
    }
}
