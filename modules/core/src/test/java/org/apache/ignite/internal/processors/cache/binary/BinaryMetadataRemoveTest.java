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

package org.apache.ignite.internal.processors.cache.binary;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 *
 */
public class BinaryMetadataRemoveTest extends GridCommonAbstractTest {
    /** Max retry cont. */
    private static final int MAX_RETRY_CONT = 10;

    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private GridTestUtils.DiscoveryHook discoveryHook;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi;

        final GridTestUtils.DiscoveryHook discoveryHook0 = discoveryHook;

        if (discoveryHook0 != null) {
            discoSpi = new TcpDiscoverySpi() {
                @Override public void setListener(@Nullable DiscoverySpiListener lsnr) {
                    if (discoveryHook0 != null)
                        super.setListener(GridTestUtils.DiscoverySpiListenerWrapper.wrap(lsnr, discoveryHook0));
                }
            };
        }
        else
            discoSpi = new TcpDiscoverySpi();

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration(new CacheConfiguration().setName(CACHE_NAME));

        return cfg;
    }

    /**
     *
     */
    protected void startCluster() throws Exception {
        startGrid("srv0");
        startGrid("srv1");
        startGrid("srv2");
        startClientGrid("cli0");
        startClientGrid("cli1");
        startClientGrid("cli2");
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startCluster();

        discoveryHook = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Tests remove not existent type and checks the exception.
     */
    @Test
    public void testRemoveNotExistentType() {
        for (Ignite testNode : G.allGrids()) {
            GridTestUtils.assertThrows(log, () -> {
                    ((IgniteEx)testNode).context().cacheObjects().removeType(
                        ((IgniteEx)testNode).context().cacheObjects().typeId("NotExistentType"));

                    return null;
                },
                IgniteException.class, "Failed to remove metadata, type not found");
        }
    }

    /**
     * Tests remove type metadata at all nodes (coordinator, server, client).
     */
    @Test
    public void testRemoveTypeOnNodes() throws Exception {
        List<IgniteEx[]> testNodeSets = new ArrayList<>();

        // Add all servers permutations to tests sets.
        for (Ignite ign0 : G.allGrids()) {
            for (Ignite ign1 : G.allGrids()) {
                for (Ignite ign2 : G.allGrids()) {
                    IgniteEx ignx0 = (IgniteEx)ign0;
                    IgniteEx ignx1 = (IgniteEx)ign1;
                    IgniteEx ignx2 = (IgniteEx)ign2;

                    if (!ignx0.context().clientNode()
                        && !ignx1.context().clientNode()
                        && !ignx2.context().clientNode())
                        testNodeSets.add(new IgniteEx[] {ignx0, ignx1, ignx2});
                }
            }
        }

        testNodeSets.add(new IgniteEx[] {grid("srv0"), grid("cli0"), grid("cli0")});
        testNodeSets.add(new IgniteEx[] {grid("cli0"), grid("cli0"), grid("cli0")});
        testNodeSets.add(new IgniteEx[] {grid("cli0"), grid("cli1"), grid("cli2")});

        for (IgniteEx[] testNodeSet : testNodeSets) {
            IgniteEx ignCreateType = testNodeSet[0];
            IgniteEx ignRemoveType = testNodeSet[1];
            IgniteEx ignRecreateType = testNodeSet[2];

            log.info("+++ Check [createOn=" + ignCreateType.name() +
                ", removeOn=" + ignRemoveType.name() + ", recreateOn=" + ignRecreateType.name());

            BinaryObjectBuilder builder0 = ignCreateType.binary().builder("Type0");

            builder0.setField("f", 1);
            builder0.build();

            delayIfClient(ignCreateType, ignRemoveType, ignRecreateType);

            removeType(ignRemoveType, "Type0");

            delayIfClient(ignCreateType, ignRemoveType, ignRecreateType);

            BinaryObjectBuilder builder1 = ignRecreateType.binary().builder("Type0");
            builder1.setField("f", "string");
            builder1.build();

            delayIfClient(ignCreateType, ignRemoveType, ignRecreateType);

            // Remove type at the end of test case.
            removeType(grid("srv0"), "Type0");

            delayIfClient(ignCreateType, ignRemoveType, ignRecreateType);
        }
    }

    /**
     * Tests reject metadata update on coordinator when remove type is processed.
     */
    @Test
    public void testChangeMetaWhenTypeRemoving() throws Exception {
        final CyclicBarrier barrier0 = new CyclicBarrier(2);
        final CyclicBarrier barrier1 = new CyclicBarrier(2);

        AtomicBoolean hookMsgs = new AtomicBoolean(true);

        discoveryHook = new GridTestUtils.DiscoveryHook() {
            @Override public void beforeDiscovery(DiscoverySpiCustomMessage msg) {
                if (!hookMsgs.get())
                    return;

                DiscoveryCustomMessage customMsg = msg == null ? null
                    : (DiscoveryCustomMessage)IgniteUtils.field(msg, "delegate");

                if (customMsg instanceof MetadataRemoveProposedMessage) {
                    try {
                        barrier0.await();

                        barrier1.await();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        // Install discovery hoot at the node 'srv1'
        stopGrid("srv1");
        IgniteEx ign = startGrid("srv1");

        discoveryHook = null;

        // Move srv2 node an the end of the discovery circle.
        stopGrid("srv2");
        startGrid("srv2");

        BinaryObjectBuilder builder0 = ign.binary().builder("Type0");

        builder0.setField("f", 1);
        builder0.build();

        GridTestUtils.runAsync(() -> {
            try {
                removeType(ign, "Type0");
            }
            catch (Exception e) {
                log.error("Unexpected exception", e);

                fail("Unexpected exception.");
            }
        });

        barrier0.await();

        GridTestUtils.assertThrowsAnyCause(log, () -> {
            BinaryObjectBuilder bld = grid("srv2").binary().builder("Type0");

            bld.setField("f1", 1);

            // Short delay guarantee that we go into update metadata before remove metadata continue processing.
            GridTestUtils.runAsync(() -> {
                try {
                    U.sleep(200);

                    hookMsgs.set(false);

                    barrier1.await();
                }
                catch (Exception e) {
                    // No-op.
                }
            });

            bld.build();

            return null;
        }, BinaryObjectException.class, "The type is removing now");
    }

    /**
     * @param ign Node to remove type.
     * @param typeName Binary type name.
     */
    protected void removeType(IgniteEx ign, String typeName) throws Exception {
        Exception err = null;

        for (int i = 0; i < MAX_RETRY_CONT; ++i) {
            try {
                ign.context().cacheObjects().removeType(ign.context().cacheObjects().typeId(typeName));

                err = null;

                break;
            }
            catch (Exception e) {
                err = e;

                U.sleep(200);
            }
        }

        if (err != null)
            throw err;
    }

    /**
     * Delay operation if an operation is executed on a client node.
     *
     * @param igns Tests nodes.
     */
    protected void delayIfClient(Ignite... igns) throws IgniteInterruptedCheckedException {
        boolean isThereCli = Arrays.stream(igns).anyMatch(ign -> ((IgniteEx)ign).context().clientNode());

        if (isThereCli)
            U.sleep(500);
    }
}
