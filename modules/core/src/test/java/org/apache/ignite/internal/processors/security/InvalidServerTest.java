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

import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.impl.TestSecurityProcessor;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.junit.Test;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Test server connection when it's permissions are removed after
 * {@link TcpDiscoveryJoinRequestMessage} processed.
 */
public class InvalidServerTest extends AbstractSecurityTest {
    /** Test server name. */
    private static final String TEST_SERVER_NAME = "test_server";

    /** */
    @Test
    public void testInvalidServer() throws Exception {
        globalAuth = true;

        startServerNode("server1");
        startServerNode("server2");

        assertThrowsWithCause(() -> startServerNode(TEST_SERVER_NAME), IgniteAuthenticationException.class);
    }

    /** */
    private IgniteEx startServerNode(String login) throws Exception {
        TestSecurityPluginProvider provider = new TestSecurityPluginProvider(login, "", ALLOW_ALL, globalAuth) {
            @Override protected GridSecurityProcessor securityProcessor(GridKernalContext ctx) {
                return new InvalidServerSecurityProcessor(ctx, super.securityProcessor(ctx));
            }
        };

        return startGrid(getConfiguration(login, provider)
            .setClientMode(false));
    }

    /** */
    static class InvalidServerSecurityProcessor extends TestSecurityProcessor.TestSecurityProcessorDelegator {
        /** */
        public InvalidServerSecurityProcessor(GridKernalContext ctx,
            GridSecurityProcessor original) {
            super(ctx, original);
        }

        /** {@inheritDoc} */
        @Override public SecurityContext authenticateNode(ClusterNode node,
            SecurityCredentials cred) throws IgniteCheckedException {
            if(TEST_SERVER_NAME.equals(cred.getLogin()) && !TEST_SERVER_NAME.equals(ctx.igniteInstanceName()))
                return null;

            return super.authenticateNode(node, cred);
        }
    }
}
