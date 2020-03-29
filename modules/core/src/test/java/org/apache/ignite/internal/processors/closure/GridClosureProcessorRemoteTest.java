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

package org.apache.ignite.internal.processors.closure;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.CA;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

/**
 * Tests execution of anonymous closures on remote nodes.
 */
@GridCommonTest(group = "Closure Processor")
public class GridClosureProcessorRemoteTest extends GridCommonAbstractTest {
    /** Number of grids started for tests. Should not be less than 2. */
    public static final int NODES_CNT = 2;

    /** Local counter that won't be affected by remote jobs. */
    private static AtomicInteger execCntr = new AtomicInteger(0);


    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

        /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setDiscoverySpi(new TcpDiscoverySpi());

        return cfg;
    }

    /**
     * @throws Exception Thrown in case of failure.
     */
    @Test
    public void testAnonymousBroadcast() throws Exception {
        Ignite g = grid(0);

        assert g.cluster().nodes().size() == NODES_CNT;

        execCntr.set(0);

        g.compute().broadcast(new CARemote() {
            @Override public void apply() {
                log.info("BROADCASTING....");

                ignite.countDownLatch("broadcast", 2, false, true).countDown();

                execCntr.incrementAndGet();
            }
        });

        assertTrue(g.countDownLatch("broadcast", 2, false, true).await(2000));
        assertEquals(1, execCntr.get());
    }

    /**
     * @throws Exception Thrown in case of failure.
     */
    @Test
    public void testAnonymousUnicast() throws Exception {
        Ignite g = grid(0);

        assert g.cluster().nodes().size() == NODES_CNT;

        execCntr.set(0);

        ClusterNode rmt = F.first(g.cluster().forRemotes().nodes());

        compute(g.cluster().forNode(rmt)).run(new CARemote() {
            @Override public void apply() {
                log.info("UNICASTING....");

                ignite.countDownLatch("unicast", 1, false, true).countDown();

                execCntr.incrementAndGet();
            }
        });

        assertTrue(g.countDownLatch("unicast", 1, false, true).await(2000));
        assertEquals(0, execCntr.get());
    }

    /**
     *
     * @throws Exception Thrown in case of failure.
     */
    @Test
    public void testAnonymousUnicastRequest() throws Exception {
        Ignite g = grid(0);

        assert g.cluster().nodes().size() == NODES_CNT;

        execCntr.set(0);

        ClusterNode rmt = F.first(g.cluster().forRemotes().nodes());
        final ClusterNode loc = g.cluster().localNode();

        compute(g.cluster().forNode(rmt)).run(new CARemote() {
            @Override public void apply() {
                message(grid(1).cluster().forNode(loc)).localListen(null, new IgniteBiPredicate<UUID, String>() {
                    @Override public boolean apply(UUID uuid, String s) {
                        log.info("Received test message [nodeId: " + uuid + ", s=" + s + ']');

                        ignite.countDownLatch("messagesPending", 1, false, true).countDown();

                        execCntr.incrementAndGet();

                        return false;
                    }
                });
            }
        });

        message(g.cluster().forNode(rmt)).send(null, "TESTING...");

        assertTrue(g.countDownLatch("messagesPending", 1, false, true).await(2000));
        assertEquals(0, execCntr.get());
    }

    /** Base class for remote tasks. */
    private abstract class CARemote extends CA {
        /** Ignite instance local to that node. */
        @IgniteInstanceResource
        protected Ignite ignite;

        /** Logger to use. */
        @LoggerResource
        protected IgniteLogger log;
    }
}
