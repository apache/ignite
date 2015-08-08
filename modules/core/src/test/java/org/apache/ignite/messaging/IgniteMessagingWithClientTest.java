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

package org.apache.ignite.messaging;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public class IgniteMessagingWithClientTest extends GridCommonAbstractTest implements Serializable {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Message topic. */
    private enum TOPIC {
        /** */
        ORDERED
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new OptimizedMarshaller(false));

        if (gridName.equals(getTestGridName(2))) {
            cfg.setClientMode(true);

            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);
        }

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMessageSendWithClientJoin() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-996");

        startGrid(0);

        Ignite ignite1 = startGrid(1);

        ClusterGroup rmts = ignite1.cluster().forRemotes();

        IgniteMessaging msg = ignite1.message(rmts);

        msg.localListen(TOPIC.ORDERED, new LocalListener());

        msg.remoteListen(TOPIC.ORDERED, new RemoteListener());

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                int iter = 0;

                while (!stop.get()) {
                    if (iter % 10 == 0)
                        log.info("Client start/stop iteration: " + iter);

                    iter++;

                    try (Ignite ignite = startGrid(2)) {
                        assertTrue(ignite.configuration().isClientMode());
                    }
                }

                return null;
            }
        }, 1, "client-start-stop");

        try {
            long stopTime = U.currentTimeMillis() + 30_000;

            int iter = 0;

            while (System.currentTimeMillis() < stopTime) {
                try {
                    ignite1.message(rmts).sendOrdered(TOPIC.ORDERED, Integer.toString(iter), 0);
                }
                catch (IgniteException e) {
                    log.info("Message send failed: " + e);
                }

                iter++;

                if (iter % 100 == 0)
                    Thread.sleep(5);
            }
        }
        finally {
            stop.set(true);
        }

        fut.get();
    }

    /**
     *
     */
    private static class LocalListener implements IgniteBiPredicate<UUID, String> {
        /** {@inheritDoc} */
        @Override public boolean apply(UUID uuid, String s) {
            return true;
        }
    }

    /**
     *
     */
    private static class RemoteListener implements IgniteBiPredicate<UUID, String> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public boolean apply(UUID nodeId, String msg) {
            ignite.message(ignite.cluster().forNodeId(nodeId)).send(TOPIC.ORDERED, msg);

            return true;
        }
    }
}
