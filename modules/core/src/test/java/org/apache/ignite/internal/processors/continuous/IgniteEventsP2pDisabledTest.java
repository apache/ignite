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

package org.apache.ignite.internal.processors.continuous;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/** Tests {@link IgniteEvents} in case peer class loading is disabled. */
public class IgniteEventsP2pDisabledTest extends GridCommonAbstractTest {
    /** Number of server nodes. */
    private static final int NODES_CNT = 2;

    /** Name of event filter class that is only available on the client node. */
    private static final String EXT_EVT_FILTER_CLS = "org.apache.ignite.tests.p2p.GridEventConsumeFilter";

    /** External classloader which is used to make event filter class unavailable on server nodes. */
    private final ClassLoader extLdr = getExternalClassLoader();

    /** Latch that indicates that {@link StopRoutineDiscoveryMessage} was handled by all nodes. */
    private final CountDownLatch stopRoutineReqHandledLatch = new CountDownLatch(NODES_CNT);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        if (getTestIgniteInstanceName(NODES_CNT).equals(igniteInstanceName))
            cfg.setClassLoader(extLdr);
        else {
            ((TestTcpDiscoverySpi)cfg.getDiscoverySpi()).discoveryHook(new GridTestUtils.DiscoveryHook() {
                @Override public void afterDiscovery(DiscoveryCustomMessage customMsg) {
                    if (customMsg instanceof StopRoutineDiscoveryMessage)
                        stopRoutineReqHandledLatch.countDown();
                }
            });
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeOrHaltFailureHandler();
    }

    /** Tests remote event listener registration in case filter class is unavailable on server nodes. */
    @Test
    public void testRemoteListenerFilterClassUnavailable() throws Exception {
        startGrids(NODES_CNT);

        IgniteEx cli = startClientGrid(NODES_CNT);

        Class<IgnitePredicate<Event>> rmtFilter = (Class<IgnitePredicate<Event>>)extLdr.loadClass(EXT_EVT_FILTER_CLS);

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> cli.events().remoteListen(null, rmtFilter.newInstance(), EVT_CACHE_OBJECT_PUT),
            ClassNotFoundException.class,
            EXT_EVT_FILTER_CLS
        );

        assertTrue(stopRoutineReqHandledLatch.await(getTestTimeout(), MILLISECONDS));
    }
}
