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

package org.apache.ignite.internal.processors.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.rest.handlers.cluster.GridBaselineCommandResponse;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.configuration.WALMode.NONE;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SUCCESS;

/**
 * Test REST with enabled authentication.
 */
public class JettyRestProcessorBaselineSelfTest extends JettyRestProcessorCommonSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", true);

        super.beforeTestsStarted();

        grid(0).cluster().baselineAutoAdjustEnabled(false);
        // We need to activate cluster.
        grid(0).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        grid(0).cluster().setBaselineTopology(grid(0).cluster().topologyVersion());
    }

    /** {@inheritDoc} */
    @Override protected String signature() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(100 * 1024 * 1024)
                .setPersistenceEnabled(true))
            .setWalMode(NONE);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /**
     * @param nodes Collection of grid nodes.
     * @return Collection of node consistent IDs for given collection of grid nodes.
     */
    private static Collection<String> nodeConsistentIds(@Nullable Collection<? extends BaselineNode> nodes) {
        if (nodes == null || nodes.isEmpty())
            return Collections.emptyList();

        return F.viewReadOnly(nodes, n -> String.valueOf(n.consistentId()));
    }

    /**
     * @param content Content to check.
     * @param baselineSz Expected baseline size.
     * @param srvsSz Expected server nodes count.
     */
    private void assertBaseline(String content, int baselineSz, int srvsSz) throws IOException {
        assertNotNull(content);
        assertFalse(content.isEmpty());

        JsonNode node = JSON_MAPPER.readTree(content);

        assertEquals(STATUS_SUCCESS, node.get("successStatus").asInt());
        assertTrue(node.get("error").isNull());

        assertNotSame(securityEnabled(), node.get("sessionToken").isNull());

        JsonNode res = node.get("response");

        assertFalse(res.isNull());

        GridBaselineCommandResponse baseline = JSON_MAPPER.treeToValue(res, GridBaselineCommandResponse.class);

        assertTrue(baseline.isActive());
        assertEquals(grid(0).cluster().topologyVersion(), baseline.getTopologyVersion());
        assertEquals(baselineSz, baseline.getBaseline().size());
        assertEqualsCollections(nodeConsistentIds(grid(0).cluster().currentBaselineTopology()), baseline.getBaseline());
        assertEquals(srvsSz, baseline.getServers().size());
        assertEqualsCollections(nodeConsistentIds(grid(0).cluster().nodes()), baseline.getServers());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBaseline() throws Exception {
        int sz = gridCount();

        assertBaseline(content(null, GridRestCommand.BASELINE_CURRENT_STATE), sz, sz);

        // Stop one node. It will stay in baseline.
        stopGrid(sz - 1);
        assertBaseline(content(null, GridRestCommand.BASELINE_CURRENT_STATE), sz, sz - 1);

        // Start one node. Server node will be added, but baseline will not change.
        startGrid(sz - 1);
        assertBaseline(content(null, GridRestCommand.BASELINE_CURRENT_STATE), sz, sz);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineSet() throws Exception {
        int sz = gridCount();

        assertBaseline(content(null, GridRestCommand.BASELINE_CURRENT_STATE), sz, sz);

        IgniteEx ignite = startGrid(sz);

        ignite.cluster().baselineAutoAdjustEnabled(false);

        assertBaseline(content(null, GridRestCommand.BASELINE_CURRENT_STATE), sz, sz + 1);

        assertBaseline(content(null, GridRestCommand.BASELINE_SET, "topVer",
            String.valueOf(grid(0).cluster().topologyVersion())), sz + 1, sz + 1);

        stopGrid(sz);

        assertBaseline(content(null, GridRestCommand.BASELINE_CURRENT_STATE), sz + 1, sz);

        assertBaseline(content(null, GridRestCommand.BASELINE_SET, "topVer",
            String.valueOf(grid(0).cluster().topologyVersion())), sz, sz);

        startGrid(sz);
        assertBaseline(content(null, GridRestCommand.BASELINE_CURRENT_STATE), sz, sz + 1);

        ArrayList<String> params = new ArrayList<>();
        int i = 1;

        for (BaselineNode n : grid(0).cluster().nodes()) {
            params.add("consistentId" + i++);
            params.add(String.valueOf(n.consistentId()));
        }

        assertBaseline(content(null, GridRestCommand.BASELINE_SET, params.toArray(new String[0])),
            sz + 1, sz + 1);

        stopGrid(sz);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineAdd() throws Exception {
        int sz = gridCount();

        assertBaseline(content(null, GridRestCommand.BASELINE_CURRENT_STATE), sz, sz);

        startGrid(sz);
        assertBaseline(content(null, GridRestCommand.BASELINE_CURRENT_STATE), sz, sz + 1);

        assertBaseline(content(null, GridRestCommand.BASELINE_ADD, "consistentId1",
            grid(sz).localNode().consistentId().toString()), sz + 1, sz + 1);

        stopGrid(sz);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineRemove() throws Exception {
        int sz = gridCount();

        assertBaseline(content(null, GridRestCommand.BASELINE_CURRENT_STATE), sz, sz);

        startGrid(sz);
        assertBaseline(content(null, GridRestCommand.BASELINE_CURRENT_STATE), sz, sz + 1);

        assertBaseline(content(null, GridRestCommand.BASELINE_SET, "topVer",
            String.valueOf(grid(0).cluster().topologyVersion())), sz + 1, sz + 1);

        String consistentId = grid(sz).localNode().consistentId().toString();

        stopGrid(sz);
        assertBaseline(content(null, GridRestCommand.BASELINE_CURRENT_STATE), sz + 1, sz);

        assertBaseline(content(null, GridRestCommand.BASELINE_REMOVE, "consistentId1",
            consistentId), sz, sz);
    }
}
