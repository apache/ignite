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

package org.apache.ignite.internal.processors.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests of determined assignments function of {@link GridServiceProcessor}.
 */
@RunWith(Parameterized.class)
public class ServiceReassignmentFunctionSelfTest {
    /** */
    private static final String TEST_SERVICE_NAME = "testServiceName";

    /** */
    private final List<ClusterNode> nodes;

    /** */
    private final List<IgniteServiceProcessor> processors;

    /** */
    @BeforeClass
    public static void setup() {
        GridTestProperties.init();
    }

    /**
     * @return Nodes count to test.
     */
    @Parameterized.Parameters(name = "{index}: nodes count={0}")
    public static Collection nodesCount() {
        return Arrays.asList(new Object[][] {{1}, {2}, {3}, {4}, {11}, {50}});
    }

    /**
     * @param nodesCnt Nodes count to test.
     */
    public ServiceReassignmentFunctionSelfTest(int nodesCnt) {
        assertTrue(nodesCnt > 0);

        nodes = new ArrayList<>();

        processors = new ArrayList<>();

        for (int i = 0; i < nodesCnt; i++)
            nodes.add(new GridTestNode(UUID.randomUUID()));

        for (int i = 0; i < nodesCnt; i++)
            processors.add(mockServiceProcessor());
    }

    /**
     * Mocks GridServiceProcessor to test method {@link GridServiceProcessor#reassign(IgniteUuid, ServiceConfiguration,
     * AffinityTopologyVersion, Map)} )}.
     */
    private IgniteServiceProcessor mockServiceProcessor() {
        GridTestKernalContext spyCtx = spy(new GridTestKernalContext(new GridTestLog4jLogger()));

        GridEventStorageManager mockEvt = mock(GridEventStorageManager.class);
        GridIoManager mockIo = mock(GridIoManager.class);

        when(spyCtx.event()).thenReturn(mockEvt);
        when(spyCtx.io()).thenReturn(mockIo);

        GridDiscoveryManager mockDisco = mock(GridDiscoveryManager.class);

        when(mockDisco.nodes(any(AffinityTopologyVersion.class))).thenReturn(new ArrayList<>(nodes));

        spyCtx.add(mockDisco);

        return new IgniteServiceProcessor(spyCtx);
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testClusterSingleton() throws Exception {
        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName(TEST_SERVICE_NAME);
        cfg.setTotalCount(1);

        runTestReassignFunction(IgniteUuid.randomUuid(), cfg, null);
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testClusterSingletonWithOldTop() throws Exception {
        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName(TEST_SERVICE_NAME);
        cfg.setTotalCount(1);

        IgniteUuid srvcId = IgniteUuid.randomUuid();

        Map<UUID, Integer> oldTop = new HashMap<>();

        ClusterNode randomNode = nodes.get(new Random().nextInt(nodes.size()));

        oldTop.put(randomNode.id(), 1);

        runTestReassignFunction(srvcId, cfg, oldTop);
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testNodeSingleton() throws Exception {
        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName(TEST_SERVICE_NAME);
        cfg.setMaxPerNodeCount(1);

        runTestReassignFunction(IgniteUuid.randomUuid(), cfg, null);
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testNodeSingletonWithOldTop() throws Exception {
        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName(TEST_SERVICE_NAME);
        cfg.setMaxPerNodeCount(1);

        IgniteUuid srvcId = IgniteUuid.randomUuid();

        Map<UUID, Integer> oldTop = new HashMap<>();

        for (ClusterNode node : nodes)
            oldTop.put(node.id(), 1);

        runTestReassignFunction(srvcId, cfg, oldTop);
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testCustomConfiguration() throws Exception {
        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName(TEST_SERVICE_NAME);
        cfg.setMaxPerNodeCount(3);
        cfg.setTotalCount(10);

        runTestReassignFunction(IgniteUuid.randomUuid(), cfg, null);
    }

    /**
     * @param cfg Service configuration to test.
     * @throws Exception In case of an error.
     */
    @Ignore
    public void runTestReassignFunction(IgniteUuid srvcId, ServiceConfiguration cfg,
        Map<UUID, Integer> oldTop) throws Exception {
        final IgniteServiceProcessor proc0 = processors.get(0);
        final AffinityTopologyVersion stubTopVer = AffinityTopologyVersion.NONE;

        Map<UUID, Integer> sut = proc0.reassign(srvcId, cfg, stubTopVer, oldTop);

        for (int idx = 1; idx < nodes.size(); idx++) {
            IgniteServiceProcessor proc = processors.get(idx);

            Map<UUID, Integer> assign = proc.reassign(srvcId, cfg, stubTopVer, oldTop);

            assertEquals(sut, assign);
        }
    }
}
