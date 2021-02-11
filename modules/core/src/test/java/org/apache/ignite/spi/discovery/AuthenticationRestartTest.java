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

package org.apache.ignite.spi.discovery;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Checks whether client is able to reconnect to restarted cluster with
 * enabled security.
 */
public class AuthenticationRestartTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setJoinTimeout(1120_000);

        cfg.setPluginProviders(new TestReconnectSecurityPluginProvider());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid("server");
        startClientGrid("client");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnect() throws Exception {
        stopGrid("server");

        final IgniteEx client = grid("client");

        waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return client.cluster().clientReconnectFuture() != null;
            }
        }, 10_000);

        startGrid("server");

        IgniteFuture<?> fut = client.cluster().clientReconnectFuture();

        assertNotNull(fut);

        fut.get();

        assertEquals(2, client.cluster().nodes().size());
    }
}
