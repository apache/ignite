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

import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;

/**
 *
 */
public class CacheConfigurationP2PTest extends GridCommonAbstractTest {
    /** */
    public static final String NODE_START_MSG = "Test external node started";

    /** */
    private static final String CLIENT_CLS_NAME =
        "org.apache.ignite.tests.p2p.startcache.CacheConfigurationP2PTestClient";

    /**
     * @throws Exception If failed.
     */
    public void testCacheConfigurationP2P() throws Exception {
        final CountDownLatch srvsReadyLatch = new CountDownLatch(2);

        final CountDownLatch clientReadyLatch = new CountDownLatch(1);

        GridJavaProcess node1 = null;
        GridJavaProcess node2 = null;
        GridJavaProcess clientNode = null;

        try {
            node1 = GridJavaProcess.exec(
                CacheConfigurationP2PTestServer.class.getName(), null,
                log,
                new CI1<String>() {
                    @Override public void apply(String s) {
                        info("Server node1: " + s);

                        if (s.contains(NODE_START_MSG))
                            srvsReadyLatch.countDown();
                    }
                },
                null,
                null,
                null
            );

            node2 = GridJavaProcess.exec(
                CacheConfigurationP2PTestServer.class.getName(), null,
                log,
                new CI1<String>() {
                    @Override public void apply(String s) {
                        info("Server node2: " + s);

                        if (s.contains(NODE_START_MSG))
                            srvsReadyLatch.countDown();
                    }
                },
                null,
                null,
                null
            );

            assertTrue(srvsReadyLatch.await(60, SECONDS));

            String str = U.getIgniteHome() + "/modules/extdata/p2p/target/classes/";

            clientNode = GridJavaProcess.exec(
                CLIENT_CLS_NAME, null,
                log,
                new CI1<String>() {
                    @Override public void apply(String s) {
                        info("Client node: " + s);

                        if (s.contains(NODE_START_MSG))
                            clientReadyLatch.countDown();
                    }
                },
                null,
                null,
                str
            );

            assertTrue(clientReadyLatch.await(60, SECONDS));

            int exitCode = clientNode.getProcess().waitFor();

            assertEquals("Unexpected exit code", 0, exitCode);
        }
        finally {
            if (node1 != null)
                node1.killProcess();

            if (node2 != null)
                node2.killProcess();

            if (clientNode != null)
                clientNode.killProcess();
        }
    }
}
