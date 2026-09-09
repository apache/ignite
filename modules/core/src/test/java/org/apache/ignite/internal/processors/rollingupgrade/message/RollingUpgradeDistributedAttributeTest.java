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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.authentication.User;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.thread.context.DistributedAttributeKey;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.OperationContextAttribute;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.MessagesPluginProvider;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.rollingupgrade.feature.TestIgniteReleaseFeatures_2_20_0.VER_2_20_0_ID_3_FEATURE;
import static org.apache.ignite.internal.processors.rollingupgrade.message.RollingUpgradeDistributedAttributeTest.TestIgniteComponent.PTR_VAL;
import static org.apache.ignite.internal.processors.rollingupgrade.message.RollingUpgradeDistributedAttributeTest.TestIgniteComponent.USR_VAL;
import static org.apache.ignite.internal.processors.rollingupgrade.message.RollingUpgradeDistributedAttributeTest.TestIgniteComponent.VER_2_19_PTR_ATTR;
import static org.apache.ignite.internal.processors.rollingupgrade.message.RollingUpgradeDistributedAttributeTest.TestIgniteComponent.VER_2_20_USR_ATTR;

/** */
public class RollingUpgradeDistributedAttributeTest extends AbstractRollingUpgradeMessageTest {
    /** */
    private static final DistributedAttributeKey VER_2_19_ATTR_KEY = createTestKey(0);

    /** */
    private static final DistributedAttributeKey VER_2_20_ATTR_KEY = createTestKey(7, VER_2_20_0_ID_3_FEATURE);

    /** {@inheritDoc} */
    @Override protected Collection<DistributedAttributeKey> distributedAttributeKeys() {
        return Arrays.asList(VER_2_19_ATTR_KEY, VER_2_20_ATTR_KEY);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName, String ver) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName, ver);

        cfg.setPluginProviders(F.concat(
            cfg.getPluginProviders(),
            new MessagesPluginProvider(TestCoreMessage.class),
            new TestIgniteComponent()));

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
    public void testNewAttributeIsCutAroundRingWithOldCoordinator() throws Exception {
        startServerNodes("2.19.0", "2.20.0", "2.20.0");

        Map<String, Received<TestCoreMessage>> rcvd = sendOverDiscovery(grid(1), TestCoreMessage.build());

        assertAttributes(PTR_VAL, null, rcvd.get(grid(0).name()));
        assertAttributes(PTR_VAL, null, rcvd.get(grid(2).name()));

        assertAttributes(PTR_VAL, USR_VAL, send(grid(1), grid(2), TestCoreMessage.build()));
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

        Map<String, Received<TestCoreMessage>> rcvd = sendOverDiscovery(grid(1), TestCoreMessage.build());

        assertAttributes(PTR_VAL, USR_VAL, rcvd.get(newVerCli.name()));
        assertAttributes(PTR_VAL, null, rcvd.get(oldVerCli.name()));

        assertAttributes(PTR_VAL, USR_VAL, send(grid(1), newVerCli, TestCoreMessage.build()));
        assertAttributes(PTR_VAL, null, send(grid(1), oldVerCli, TestCoreMessage.build()));
    }

    /** */
    private void checkMutualSend(IgniteEx first, IgniteEx second, WALPointer expPtr, @Nullable User expUsr) throws Exception {
        assertAttributes(expPtr, expUsr, send(first, second, TestCoreMessage.build()));
        assertAttributes(expPtr, expUsr, send(second, first, TestCoreMessage.build()));

        assertAttributes(expPtr, expUsr, sendOverDiscovery(first, TestCoreMessage.build()).get(second.name()));
        assertAttributes(expPtr, expUsr, sendOverDiscovery(second, TestCoreMessage.build()).get(first.name()));
    }

    /** {@inheritDoc} */
    @Override protected <T extends DiscoveryCustomMessage> Map<String, Received<T>> sendOverDiscovery(
        IgniteEx from,
        T msg
    ) throws Exception {
        try (Scope ignored = OperationContext.set(VER_2_19_PTR_ATTR, PTR_VAL, VER_2_20_USR_ATTR, USR_VAL)) {
            return super.sendOverDiscovery(from, msg);
        }
    }

    /** {@inheritDoc} */
    @Override protected <T extends Message> Received<T> send(IgniteEx from, IgniteEx to, T msg) throws Exception {
        try (Scope ignored = OperationContext.set(VER_2_19_PTR_ATTR, PTR_VAL, VER_2_20_USR_ATTR, USR_VAL)) {
            return super.send(from, to, msg);
        }
    }

    /** */
    private static void assertAttributes(WALPointer expBase, @Nullable User expNew, Received<?> rcvd) {
        assertEquals(expBase, rcvd.attribute(VER_2_19_PTR_ATTR));
        assertEquals(expNew, rcvd.attribute(VER_2_20_USR_ATTR));
    }

    /** */
    static class TestIgniteComponent extends AbstractTestPluginProvider {
        /** */
        public static final OperationContextAttribute<WALPointer> VER_2_19_PTR_ATTR = OperationContextAttribute.newInstance();

        /** */
        public static final OperationContextAttribute<User> VER_2_20_USR_ATTR = OperationContextAttribute.newInstance();

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

            kctx.operationContextDispatcher().registerDistributedAttribute(VER_2_19_ATTR_KEY, VER_2_19_PTR_ATTR);

            if (kctx.localNodeFeatures().contains(VER_2_20_0_ID_3_FEATURE))
                kctx.operationContextDispatcher().registerDistributedAttribute(VER_2_20_ATTR_KEY, VER_2_20_USR_ATTR);
        }
    }
}
