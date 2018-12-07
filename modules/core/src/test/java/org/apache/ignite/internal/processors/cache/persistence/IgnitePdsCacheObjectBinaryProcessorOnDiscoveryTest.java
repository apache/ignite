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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgnitePdsCacheObjectBinaryProcessorOnDiscoveryTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if ("client".equals(igniteInstanceName))
            cfg.setClientMode(true).setFailureHandler(new StopNodeFailureHandler());

        return cfg.setDiscoverySpi(new TcpDiscoverySpi()
                .setIpFinder(IP_FINDER))
                .setDataStorageConfiguration(new DataStorageConfiguration()
                        .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                                .setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests that joining node metadata correctly handled on client.
     * @throws Exception If fails.
     */
    public void testJoiningNodeBinaryMetaOnClient() throws Exception {
        IgniteEx ig0 = (IgniteEx)startGrids(2);

        ig0.cluster().active(true);

        addBinaryType(ig0, "test_1", new IgniteBiTuple<>("name", String.class));

        stopGrid(0);

        Ignite ig1 = grid(1);

        // Modify existing type.
        addBinaryType(ig1, "test_1", new IgniteBiTuple<>("id", Integer.class));

        // Add new type.
        addBinaryType(ig1, "test_2", new IgniteBiTuple<>("name", String.class));

        stopGrid(1);

        startGrid(0);

        IgniteEx client = startGrid(getConfiguration("client"));

        startGrid(1);

        awaitPartitionMapExchange();

        // Check that new metadata from grid_1 was handled without NPE on client.
        assertNull(client.context().failure().failureContext());

        // Check that metadata from grid_1 correctly loaded on client.
        assertTrue(client.binary().type("test_1").fieldNames().containsAll(Arrays.asList("id", "name")));
        assertTrue(client.binary().type("test_2").fieldNames().contains("name"));
    }

    /**
     * @param ig Ig.
     * @param typeName Type name.
     * @param fields Fields.
     */
    @SafeVarargs
    private final BinaryObject addBinaryType(Ignite ig, String typeName, IgniteBiTuple<String, Class<?>>... fields) {
        BinaryObjectBuilder builder = ig.binary().builder(typeName);

        if (fields != null) {
            for (IgniteBiTuple<String,Class<?>> field: fields)
                builder.setField(field.get1(), field.get2());
        }

        return builder.build();
    }
}
