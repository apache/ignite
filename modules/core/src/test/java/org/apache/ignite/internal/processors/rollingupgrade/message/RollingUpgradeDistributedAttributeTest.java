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

package org.apache.ignite.internal.processors.rollingupgrade.message;

import java.util.Map;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.authentication.User;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.thread.context.DistributedAttributeKey;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.OperationContextAttribute;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.rollingupgrade.feature.TestIgniteReleaseFeatures_2_19_0.VER_2_19_0_ID_1_FEATURE;
import static org.apache.ignite.internal.processors.rollingupgrade.feature.TestIgniteReleaseFeatures_2_20_0.VER_2_20_0_ID_3_FEATURE;
import static org.apache.ignite.internal.processors.rollingupgrade.message.RollingUpgradeDistributedAttributeTest.TestIgniteComponent.PTR_ATTR;
import static org.apache.ignite.internal.processors.rollingupgrade.message.RollingUpgradeDistributedAttributeTest.TestIgniteComponent.PTR_VAL;
import static org.apache.ignite.internal.processors.rollingupgrade.message.RollingUpgradeDistributedAttributeTest.TestIgniteComponent.USR_VAL;
import static org.apache.ignite.internal.processors.rollingupgrade.message.RollingUpgradeDistributedAttributeTest.TestIgniteComponent.VER_2_19_0_PTR_ATTR;
import static org.apache.ignite.internal.processors.rollingupgrade.message.RollingUpgradeDistributedAttributeTest.TestIgniteComponent.VER_2_20_USR_ATTR;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessageType.CORE_MSG;

/** */
public class RollingUpgradeDistributedAttributeTest extends AbstractRollingUpgradeMessageTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName, String ver) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName, ver);

        cfg.setPluginProviders(F.concat(cfg.getPluginProviders(), new TestIgniteComponent()));

        return cfg;
    }

    /** */
    @Test
    public void testNewAttributeIsCutForOldPeer() throws Exception {
        startServerNodes("2.19.0", "2.20.0");

        checkMutualSend(grid(0), grid(1), PTR_VAL, null);
    }

    /** */
    @Test
    public void testNewAttributeIsCutForPeerReadingCompactLayout() throws Exception {
        startServerNodes("2.19.2", "2.20.0");

        checkMutualSend(grid(0), grid(1), PTR_VAL, null);
    }

    /** */
    @Test
    public void testBothAttributesReachPeerOfSameNewRelease() throws Exception {
        startServerNodes("2.20.0", "2.20.0");

        checkMutualSend(grid(0), grid(1), PTR_VAL, USR_VAL);
    }

    /** */
    @Test
    public void testNewAttributeIsForwardedAroundRingThroughOldCoordinator() throws Exception {
        startServerNodes("2.19.0", "2.20.0", "2.20.0");

        try (Scope ignored = withTestAttributes()) {
            Map<String, Received<TestDiscoveryMessage>> rcvd = sendOverDiscovery(grid(1), CORE_MSG);

            assertAttributes(PTR_VAL, null, rcvd.get(grid(0).name()));
            assertAttributes(PTR_VAL, USR_VAL, rcvd.get(grid(2).name()));
        }
    }

    /** */
    @Test
    public void testNewAttributeIsForwardedThroughOldNode() throws Exception {
        startServerNodes("2.19.0", "2.20.0", "2.20.0");

        try (Scope ignored = withTestAttributes()) {
            assertAttributes(PTR_VAL, USR_VAL, sendThroughChain(grid(1), grid(0), grid(2)));
        }
    }

    /** */
    @Test
    public void testNewAttributeAloneIsForwardedThroughOldNode() throws Exception {
        startServerNodes("2.19.0", "2.20.0", "2.20.0");

        try (Scope ignored = OperationContext.set(VER_2_20_USR_ATTR, USR_VAL)) {
            assertAttributes(null, USR_VAL, sendThroughChain(grid(1), grid(0), grid(2)));
        }
    }

    /** */
    @Test
    public void testNewAttributeIsForwardedThroughTwoOldNodes() throws Exception {
        startServerNodes("2.19.0", "2.19.0", "2.20.0", "2.20.0");

        try (Scope ignored = withTestAttributes()) {
            assertAttributes(PTR_VAL, USR_VAL, sendThroughChain(grid(2), grid(0), grid(1), grid(3)));
        }
    }

    /** */
    @Test
    public void testNewAttributeIsForwardedThroughOldNodeAcrossCarriers() throws Exception {
        startServerNodes("2.19.0", "2.20.0", "2.20.0");

        try (Scope ignored = withTestAttributes()) {
            Received<TestDiscoveryMessage> rcvd = sendOverDiscovery(grid(1), CORE_MSG).get(grid(0).name());

            try (Scope ignored0 = rcvd.restoreOperationContext()) {
                assertAttributes(PTR_VAL, USR_VAL, send(grid(0), grid(2), CORE_MSG));
            }

            rcvd = send(grid(1), grid(0), CORE_MSG);

            try (Scope ignored0 = rcvd.restoreOperationContext()) {
                assertAttributes(PTR_VAL, USR_VAL, sendOverDiscovery(grid(0), CORE_MSG).get(grid(2).name()));
            }
        }
    }

    /** */
    @Test
    public void testNewAttributeReachesClientThroughOldRouter() throws Exception {
        IgniteEx oldVerSrv = startGrid(0, "2.19.0");

        ru(oldVerSrv).enableVersionUpgrade();

        IgniteEx cli = startClientGrid(1, "2.20.0");
        IgniteEx newVerSrv = startGrid(2, "2.20.0");

        assertEquals(oldVerSrv.localNode().id(), ((TcpDiscoveryNode)cli.localNode()).clientRouterNodeId());

        try (Scope ignored = withTestAttributes()) {
            Map<String, Received<TestDiscoveryMessage>> rcvd = sendOverDiscovery(newVerSrv, CORE_MSG);

            assertAttributes(PTR_VAL, null, rcvd.get(oldVerSrv.name()));
            assertAttributes(PTR_VAL, USR_VAL, rcvd.get(cli.name()));
        }
    }

    /** */
    @Test
    public void testNewAttributeIsLostThroughNodeWithoutRawFields() throws Exception {
        startServerNodes("2.18.0", "2.19.0", "2.19.0");

        try (Scope ignored = OperationContext.set(PTR_ATTR, PTR_VAL, VER_2_19_0_PTR_ATTR, PTR_VAL)) {
            Received<TestDiscoveryMessage> rcvd = sendThroughChain(grid(1), grid(0), grid(2));

            assertEquals(PTR_VAL, rcvd.attribute(PTR_ATTR));
            assertNull(rcvd.attribute(VER_2_19_0_PTR_ATTR));
        }
    }

    /** */
    @Test
    public void testNewAttributeReachesOnlyClientOfNewRelease() throws Exception {
        startGrid(0, "2.19.0");
        startGrid(1, "2.19.0");

        ru(1).enableVersionUpgrade();

        upgradeNodeVersion(0, "2.20.0");
        upgradeNodeVersion(1, "2.20.0");

        IgniteEx newVerCli = startClientGrid(2, "2.20.0");
        IgniteEx oldVerCli = startClientGrid(3, "2.19.0");

        try (Scope ignored = withTestAttributes()) {
            Map<String, Received<TestDiscoveryMessage>> rcvd = sendOverDiscovery(grid(1), CORE_MSG);

            assertAttributes(PTR_VAL, USR_VAL, rcvd.get(newVerCli.name()));
            assertAttributes(PTR_VAL, null, rcvd.get(oldVerCli.name()));

            assertAttributes(PTR_VAL, USR_VAL, send(grid(1), newVerCli, CORE_MSG));
            assertAttributes(PTR_VAL, null, send(grid(1), oldVerCli, CORE_MSG));
        }
    }

    /** */
    private void checkMutualSend(IgniteEx first, IgniteEx second, WALPointer expPtr, @Nullable User expUsr) throws Exception {
        try (Scope ignored = withTestAttributes()) {
            assertAttributes(expPtr, expUsr, send(first, second, CORE_MSG));
            assertAttributes(expPtr, expUsr, send(second, first, CORE_MSG));

            assertAttributes(expPtr, expUsr, sendOverDiscovery(first, CORE_MSG).get(second.name()));
            assertAttributes(expPtr, expUsr, sendOverDiscovery(second, CORE_MSG).get(first.name()));
        }
    }

    /** */
    private Received<TestDiscoveryMessage> sendThroughChain(IgniteEx... nodes) throws Exception {
        Received<TestDiscoveryMessage> rcvd = send(nodes[0], nodes[1], CORE_MSG);

        for (int hop = 2; hop < nodes.length; hop++) {
            try (Scope ignored = rcvd.restoreOperationContext()) {
                rcvd = send(nodes[hop - 1], nodes[hop], CORE_MSG);
            }
        }

        return rcvd;
    }

    /** */
    private Scope withTestAttributes() {
        return OperationContext.set(PTR_ATTR, PTR_VAL, VER_2_20_USR_ATTR, USR_VAL);
    }

    /** */
    private void assertAttributes(@Nullable WALPointer expBase, @Nullable User expNew, Received<?> rcvd) {
        assertEquals(expBase, rcvd.attribute(PTR_ATTR));
        assertEquals(expNew, rcvd.attribute(VER_2_20_USR_ATTR));
    }

    /** */
    static class TestIgniteComponent extends AbstractTestPluginProvider {
        /** */
        public static final OperationContextAttribute<WALPointer> PTR_ATTR = OperationContextAttribute.newInstance();

        /** */
        public static final OperationContextAttribute<User> VER_2_20_USR_ATTR = OperationContextAttribute.newInstance();

        /** */
        public static final OperationContextAttribute<WALPointer> VER_2_19_0_PTR_ATTR = OperationContextAttribute.newInstance();

        /** */
        public static final WALPointer PTR_VAL = new WALPointer(1, 1, 1);

        /** */
        public static final User USR_VAL = User.create("1", "1");

        /** {@inheritDoc} */
        @Override public String name() {
            return "TestIgniteComponent";
        }

        /** {@inheritDoc} */
        @Override public void start(PluginContext ctx) {
            GridKernalContext kctx = ((IgniteEx)ctx.grid()).context();

            kctx.distributedOperationContext().register(new DistributedAttributeKey(0), PTR_ATTR);

            if (kctx.localNodeFeatures().contains(VER_2_19_0_ID_1_FEATURE))
                kctx.distributedOperationContext().register(new DistributedAttributeKey(6, VER_2_19_0_ID_1_FEATURE), VER_2_19_0_PTR_ATTR);

            if (kctx.localNodeFeatures().contains(VER_2_20_0_ID_3_FEATURE))
                kctx.distributedOperationContext().register(new DistributedAttributeKey(7, VER_2_20_0_ID_3_FEATURE), VER_2_20_USR_ATTR);
        }
    }
}
