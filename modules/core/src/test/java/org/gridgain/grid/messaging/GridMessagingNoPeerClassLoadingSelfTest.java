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

package org.gridgain.grid.messaging;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.config.*;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Tests for Messaging public API with disabled
 * peer class loading.
 */
public class GridMessagingNoPeerClassLoadingSelfTest extends GridMessagingSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /**
     * Checks that the message, loaded with external
     * class loader, won't be unmarshalled on remote node, because
     * peer class loading is disabled.
     *
     * @throws Exception If error occurs.
     */
    @Override public void testSendMessageWithExternalClassLoader() throws Exception {
        URL[] urls = new URL[] { new URL(GridTestProperties.getProperty("p2p.uri.cls")) };

        ClassLoader extLdr = new URLClassLoader(urls);

        Class rcCls = extLdr.loadClass(EXT_RESOURCE_CLS_NAME);

        final AtomicBoolean error = new AtomicBoolean(false); //to make it modifiable

        final CountDownLatch rcvLatch = new CountDownLatch(1);

        ignite2.message().remoteListen("", new P2<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                try {
                    log.info("Received new message [msg=" + msg + ", senderNodeId=" + nodeId + ']');

                    if (!nodeId.equals(ignite1.cluster().localNode().id())) {
                        log.error("Unexpected sender node: " + nodeId);

                        error.set(true);

                        return false;
                    }

                    return true;
                }
                finally {
                    rcvLatch.countDown();
                }
            }
        });

        message(ignite1.cluster().forRemotes()).send(null, Collections.singleton(rcCls.newInstance()));

        /*
            We shouldn't get a message, because remote node won't be able to
            unmarshal it (peer class loading is disabled.)
         */
        assertFalse(rcvLatch.await(3, TimeUnit.SECONDS));
    }
}
