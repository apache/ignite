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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.deployment.GridDeploymentResponse;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class CacheContinuousQueryLongP2PTest extends CacheContinuousQueryOperationP2PTest {
    /** */
    private static volatile int delay;

    /** {@inheritDoc} */
    @Override protected CommunicationSpi communicationSpi() {
        return new P2PDelayingCommunicationSpi();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        delay = 300;

        super.beforeTest();
    }

    /**
     * Checks that a node start is not blocked by peer class loading of the continuous query remote filter.
     *
     * @throws Exception If failed.
     */
    @Test(timeout = 60_000)
    public void testLongP2PClassLoadingDoesntBlockNodeStart() throws Exception {
        delay = 3_000;

        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED, 1, ATOMIC);
        IgniteCache cache = grid(NODES - 1).getOrCreateCache(ccfg.getName());

        ContinuousQuery<Integer, Integer> qry = continuousQuery();

        cache.query(qry);

        AtomicReference<String> err = new AtomicReference<>();

        IgniteInternalFuture<?> startFut = GridTestUtils.runAsync(() -> {
            try {
                startGrid(NODES);
            }
            catch (Exception e) {
                err.set(e.getMessage());

                e.printStackTrace();
            }
        });

        startFut.get(5, TimeUnit.SECONDS);

        assertNull("Error occurred when starting a node: " + err.get(), err.get());
    }

    /**
     * @return Continuous query with remote filter from an external class loader.
     * @throws Exception If failed.
     */
    private ContinuousQuery<Integer, Integer> continuousQuery() throws Exception {
        final Class<Factory<CacheEntryEventFilter>> evtFilterFactoryCls =
            (Class<Factory<CacheEntryEventFilter>>)getExternalClassLoader().
                loadClass("org.apache.ignite.tests.p2p.CacheDeploymentEntryEventFilterFactory");

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        qry.setLocalListener((evt) -> {});

        qry.setRemoteFilterFactory(
            (Factory<? extends CacheEntryEventFilter<Integer, Integer>>)(Object)evtFilterFactoryCls.newInstance());

        return qry;
    }

    /**
     * TcpCommunicationSpi
     */
    private static class P2PDelayingCommunicationSpi extends TcpCommunicationSpi {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            if (isDeploymentResponse((GridIoMessage) msg)) {
                log.info(">>> Delaying deployment message: " + msg);

                try {
                    Thread.sleep(delay);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            super.sendMessage(node, msg, ackC);
        }

        /**
         * Checks if it is a p2p deployment response.
         *
         * @param msg Message to check.
         * @return {@code True} if this is a p2p response.
         */
        private boolean isDeploymentResponse(GridIoMessage msg) {
            Object origMsg = msg.message();

            return origMsg instanceof GridDeploymentResponse;
        }
    }
}
