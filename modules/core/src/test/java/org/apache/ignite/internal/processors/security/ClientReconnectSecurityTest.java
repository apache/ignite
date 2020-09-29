/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.security;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridJobExecuteRequest;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.communication.GridIoSecurityAwareMessage;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;

/**
 * Security tests related with client reconnect.
 */
public class ClientReconnectSecurityTest extends AbstractCacheOperationPermissionCheckTest {
    /** Cli sender uuid. */
    private static final AtomicReference<UUID> cliSenderUUID = new AtomicReference<>();

    /** Record uuid. */
    private static final AtomicBoolean recordUUID = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TcpCommunicationSpi() {
            @Override public void sendMessage(ClusterNode node, Message msg,
                IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {

                if (msg instanceof GridIoMessage && recordUUID.get() && ((GridIoMessage)msg).message() instanceof GridJobExecuteRequest)
                    cliSenderUUID.set(((GridIoSecurityAwareMessage)msg).secSubjId());

                super.sendMessage(node, msg, ackC);
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids(true);
    }

    /**
     * If the node reconnects to the cluster, the node changes its node ID. We need to reset this ID to internal
     * structures.
     */
    @Test
    public void testInvalidateNodeIdFromSecurityThreadLocal() throws Exception {
        IgniteEx cliNode = startGrid("cli",
            SecurityPermissionSetBuilder.create()
                .appendCachePermissions(CACHE_NAME, CACHE_READ, CACHE_PUT)
                .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS).build(), true);

        // Init local structures
        CountDownLatch sndTask = new CountDownLatch(1);
        CountDownLatch stopSrv = new CountDownLatch(1);
        new Thread(() -> {
            try {
                cliNode.cache(CACHE_NAME).size(CachePeekMode.ALL);

                stopSrv.countDown();

                sndTask.await();

                cliNode.cache(CACHE_NAME).size(CachePeekMode.ALL);
            }
            catch (Throwable e) {
                log.error("Test fail", e);
            }
        }).start();

        stopSrv.await();

        // Trigger reconnect
        stopGrid("server");

        IgniteEx srv = startGridAllowAll("server");

        srv.cluster().active(true);

        // Wait reconnect
        final CountDownLatch waitReconnect = new CountDownLatch(1);
        cliNode.events().localListen(event -> {
            waitReconnect.countDown();

            return true;
        }, EVT_CLIENT_NODE_RECONNECTED);

        waitReconnect.await();

        // Send task again
        recordUUID.set(true);

        sndTask.countDown();

        // Wait and check result
        assertTrue(GridTestUtils.waitForCondition(() -> cliSenderUUID.get() != null, 10_000));

        assertEquals(cliSenderUUID.get(), cliNode.localNode().id());
    }
}
