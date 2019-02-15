/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.messaging;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.P2;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.junit.Test;

/**
 * Tests for Messaging public API with disabled
 * peer class loading.
 */
public class GridMessagingNoPeerClassLoadingSelfTest extends GridMessagingSelfTest {
    /** */
    private static CountDownLatch rcvLatch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

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
    @Test
    @Override public void testSendMessageWithExternalClassLoader() throws Exception {
        URL[] urls = new URL[] { new URL(GridTestProperties.getProperty("p2p.uri.cls")) };

        ClassLoader extLdr = new URLClassLoader(urls);

        Class rcCls = extLdr.loadClass(EXT_RESOURCE_CLS_NAME);

        final AtomicBoolean error = new AtomicBoolean(false); //to make it modifiable

        rcvLatch = new CountDownLatch(1);

        ignite2.message().remoteListen(null, new P2<UUID, Object>() {
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
