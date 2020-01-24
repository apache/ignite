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


package org.apache.ignite.internal.processors.security;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginConfiguration;
import org.apache.ignite.internal.processors.security.impl.TestSecurityProcessor;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Test server connection when it's permissions are removed after
 * {@link TcpDiscoveryJoinRequestMessage} processed.
 */
public class InvalidServerTest extends AbstractSecurityTest {
    /** Test server name. */
    private static final String TEST_SERVER_NAME = "test_server";

    /** Critical failures flag. */
    private final AtomicBoolean criticalFailuresFlag = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName,
        TestSecurityPluginConfiguration pluginCfg) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName, pluginCfg);

        cfg.setDiscoverySpi(new TcpDiscoverySpi() {
            @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
                if (msg instanceof TcpDiscoveryNodeAddedMessage && msg.verified())
                    TestSecurityProcessor.PERMS.remove(new SecurityCredentials(TEST_SERVER_NAME, ""));
            }
        });

        cfg.setFailureHandler((ignite, failureContext) -> {
            criticalFailuresFlag.set(true);

            return false;
        });

        return cfg;
    }

    /** */
    @Test
    public void testInvalidServer() throws Exception {
        globalAuth = true;

        startGridAllowAll("server1");
        startGridAllowAll("server2");

        assertThrowsWithCause(() -> startGridAllowAll(TEST_SERVER_NAME), IgniteAuthenticationException.class);

        assertFalse(criticalFailuresFlag.get());
    }
}
