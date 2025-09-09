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

package org.apache.ignite.internal.processors.security.cluster;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.systemPermissions;

/** */
public class NodeJoinPermissionsTest extends AbstractSecurityTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** */
    private IgniteConfiguration configuration(int idx, SecurityPermission... sysPermissions) throws Exception {
        String login = getTestIgniteInstanceName(idx);

        return getConfiguration(
            login,
            new TestSecurityPluginProvider(
                login,
                "",
                systemPermissions(sysPermissions),
                false
            ));
    }

    /** */
    @Test
    public void testNodeJoinPermissions() throws Exception {
        // The first node can start successfully without any permissions granted.
        startGrid(configuration(0));

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> startGrid(configuration(1)),
            IgniteSpiException.class,
            "Node is not authorized to join as a server node"
        );

        assertEquals(1, grid(0).cluster().nodes().size());

        startGrid(configuration(1, JOIN_AS_SERVER));

        assertEquals(2, grid(0).cluster().nodes().size());

        // Client node can join cluster without any permissions granted.
        startClientGrid(configuration(2));

        assertEquals(3, grid(0).cluster().nodes().size());

        // Client node can reconnect without any permissions granted.
        CountDownLatch clientNodeReconnectedLatch = new CountDownLatch(1);

        grid(2).events().localListen(evt -> {
            clientNodeReconnectedLatch.countDown();

            return true;
        }, EVT_CLIENT_NODE_RECONNECTED);

        ignite(0).context().discovery().failNode(nodeId(2), null);

        assertTrue(clientNodeReconnectedLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));

        assertEquals(3, grid(0).cluster().nodes().size());
    }
}
