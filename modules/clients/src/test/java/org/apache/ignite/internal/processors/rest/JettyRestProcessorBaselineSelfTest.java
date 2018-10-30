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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.rest.handlers.cluster.GridBaselineCommandResponse;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.toSet;
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
     * @param nodes Nodes to process.
     * @return Collection of consistentIds.
     */
    private static Collection<String> consistentIdMapper(@Nullable Collection<? extends BaselineNode> nodes) {
        return Optional.ofNullable(nodes).orElseGet(Collections::emptyList)
            .stream().map(n -> String.valueOf(n.consistentId()))
            .collect(toSet());
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
        assertEqualsCollections(consistentIdMapper(grid(0).cluster().currentBaselineTopology()), new HashSet<>(baseline.getBaseline()));
        assertEquals(srvsSz, baseline.getServers().size());
        assertEqualsCollections(consistentIdMapper(grid(0).cluster().nodes()), new HashSet<>(baseline.getServers()));
    }

    /**
     * @throws Exception If failed.
     */
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
    public void testBaselineSet() throws Exception {
        assertBaseline(content(null, GridRestCommand.BASELINE_CURRENT_STATE), gridCount(), gridCount());

        startGrid(gridCount());
        assertBaseline(content(null, GridRestCommand.BASELINE_CURRENT_STATE), gridCount(), gridCount() + 1);

        assertBaseline(content(null, GridRestCommand.BASELINE_SET, "topVer",
            String.valueOf(grid(0).cluster().topologyVersion())), gridCount() + 1, gridCount() + 1);

        stopGrid(gridCount());

        assertBaseline(content(null, GridRestCommand.BASELINE_CURRENT_STATE), gridCount() + 1, gridCount());

        assertBaseline(content(null, GridRestCommand.BASELINE_SET, "topVer",
            String.valueOf(grid(0).cluster().topologyVersion())), gridCount(), gridCount());

        startGrid(gridCount());
        assertBaseline(content(null, GridRestCommand.BASELINE_CURRENT_STATE), gridCount(), gridCount() + 1);

        ArrayList<String> params = new ArrayList<>();
        int i = 1;

        for (BaselineNode n : grid(0).cluster().nodes()) {
            params.add("consistentId" + i++);
            params.add(String.valueOf(n.consistentId()));
        }

        assertBaseline(content(null, GridRestCommand.BASELINE_SET, params.toArray(new String[0])),
            gridCount() + 1, gridCount() + 1);

        stopGrid(gridCount());
    }

    /**
     * @throws Exception If failed.
     */
    public void testBaselineAdd() throws Exception {
        assertBaseline(content(null, GridRestCommand.BASELINE_CURRENT_STATE), gridCount(), gridCount());

        startGrid(gridCount());
        assertBaseline(content(null, GridRestCommand.BASELINE_CURRENT_STATE), gridCount(), gridCount() + 1);

        assertBaseline(content(null, GridRestCommand.BASELINE_ADD, "consistentId1",
            grid(gridCount()).localNode().consistentId().toString()), gridCount() + 1, gridCount() + 1);
        
        stopGrid(gridCount());
    }

    /**
     * @throws Exception If failed.
     */
    public void testBaselineRemove() throws Exception {
        assertBaseline(content(null, GridRestCommand.BASELINE_CURRENT_STATE), gridCount(), gridCount());

        startGrid(gridCount());
        assertBaseline(content(null, GridRestCommand.BASELINE_CURRENT_STATE), gridCount(), gridCount() + 1);

        assertBaseline(content(null, GridRestCommand.BASELINE_SET, "topVer",
            String.valueOf(grid(0).cluster().topologyVersion())), gridCount() + 1, gridCount() + 1);

        stopGrid(gridCount());
    }
}
