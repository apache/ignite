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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.SecurityAwareCustomMessageWrapper;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.MessagesPluginProvider;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TestBlockingTcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.spi.discovery.tcp.TestBlockingTcpDiscoverySpi.blockingDiscovery;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class NodeSecurityContextPropagationTest extends GridCommonAbstractTest {
    /** */
    private static final Collection<UUID> TEST_MESSAGE_ACCEPTED_NODES = new HashSet<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeOrHaltFailureHandler())
            .setLocalEventListeners(
                Collections.singletonMap(e -> {
                    DiscoveryCustomEvent discoEvt = (DiscoveryCustomEvent)e;

                    if (discoEvt.customMessage() instanceof TestDiscoveryAcknowledgeMessage)
                        TEST_MESSAGE_ACCEPTED_NODES.add(discoEvt.node().id());

                    return true;
                }, new int[] {EVT_DISCOVERY_CUSTOM_EVT})
            )
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(100L * 1024 * 1024)))
            .setAuthenticationEnabled(true);

        cfg.setDiscoverySpi(new TestBlockingTcpDiscoverySpi(new TcpDiscoveryVmIpFinder()
            .setAddresses(Collections.singleton("127.0.0.1:47500"))));

        cfg.setPluginProviders(new MessagesPluginProvider(TestDiscoveryMessage.class, TestDiscoveryAcknowledgeMessage.class));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testProcessCustomDiscoveryMessageFromLeftNode() throws Exception {
        IgniteEx crd = startGrid(0);

        IgniteEx cli = startClientGrid(11);

        IgniteEx srv = startGrid(1);

        CountDownLatch cliLefEvtProcessedByCoordinator = new CountDownLatch(1);

        crd.events().localListen(
            evt -> {
                cliLefEvtProcessedByCoordinator.countDown();

                return true;
            },
            EVT_NODE_LEFT
        );

        blockingDiscovery(srv).block();

        cli.context().discovery().sendCustomEvent(new TestDiscoveryMessage());

        waitForCondition(() -> anyReceivedMessageMatch(srv, msg -> msg instanceof TestDiscoveryMessage), getTestTimeout());

        runAsync(() -> stopGrid(11));

        cliLefEvtProcessedByCoordinator.await();

        waitForCondition(() -> anyReceivedMessageMatch(srv, msg -> msg instanceof TcpDiscoveryNodeLeftMessage), getTestTimeout());

        runAsync(() -> startGrid(2));

        waitForCondition(() -> anyReceivedMessageMatch(srv, msg -> isDiscoveryNodeAddedMessage(msg, 2)), getTestTimeout());

        runAsync(() -> startGrid(3));

        waitForCondition(() -> anyReceivedMessageMatch(srv, msg -> isDiscoveryNodeAddedMessage(msg, 3)), getTestTimeout());

        blockingDiscovery(srv).unblock();

        waitForCondition(
            () -> grid(0).cluster().nodes().size() == 4
                && TEST_MESSAGE_ACCEPTED_NODES.contains(grid(2).cluster().localNode().id()),
            getTestTimeout());
    }

    /** */
    private boolean isDiscoveryNodeAddedMessage(Object msg, int joiningNdeIdx) {
        return msg instanceof TcpDiscoveryNodeAddedMessage &&
            Objects.equals(getTestIgniteInstanceName(joiningNdeIdx),
                ((TcpDiscoveryNodeAddedMessage)msg).node().attribute(ATTR_IGNITE_INSTANCE_NAME));
    }

    /** */
    private boolean anyReceivedMessageMatch(IgniteEx ignite, Predicate<Object> predicate) {
        for (TcpDiscoveryAbstractMessage msg : blockingDiscovery(ignite).messageQueue()) {
            Object unwrappedMsg = msg;

            if (msg instanceof TcpDiscoveryCustomEventMessage) {
                DiscoverySpiCustomMessage customMsg = ((TcpDiscoveryCustomEventMessage)msg).message();

                assert customMsg instanceof SecurityAwareCustomMessageWrapper;

                unwrappedMsg = U.unwrapCustomMessage(customMsg);
            }

            if (predicate.test(unwrappedMsg))
                return true;
        }

        return false;
    }
}
