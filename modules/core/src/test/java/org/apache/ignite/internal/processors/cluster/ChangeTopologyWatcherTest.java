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

package org.apache.ignite.internal.processors.cluster;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 *
 */
@GridCommonTest(group = "Kernal Self")
public class ChangeTopologyWatcherTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_NAME = "TEST_NAME";
    /** */
    private static int AUTO_ADJUST_TIMEOUT = 5000;

    /**
     * @throws Exception if failed.
     */
    @Before
    public void before() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        AUTO_ADJUST_TIMEOUT = 5000;
    }

    /**
     * @throws Exception if failed.
     */
    @After
    public void after() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize(500L * 1024 * 1024);

        cfg.setDataStorageConfiguration(storageCfg);

        return cfg;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBaselineAutoAdjustAfterNodeLeft() throws Exception {
        Ignite ignite0 = startGrids(2);

        ignite0.cluster().active(true);

        ignite0.cluster().baselineAutoAdjustTimeout(AUTO_ADJUST_TIMEOUT);

        Set<Object> initBaseline = ignite0.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        stopGrid(1);

        Set<Object> nodeLeftBaseline = ignite0.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        assertEquals(initBaseline, nodeLeftBaseline);

        assertTrue(waitForCondition(
            () -> isCurrentBaselineFromOneNode(ignite0),
            AUTO_ADJUST_TIMEOUT * 2
        ));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBaselineAutoAdjustSinceSecondNodeLeft() throws Exception {
        Ignite ignite0 = startGrids(3);

        ignite0.cluster().active(true);

        ignite0.cluster().baselineAutoAdjustTimeout(AUTO_ADJUST_TIMEOUT);

        Set<Object> initBaseline = ignite0.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        stopGrid(1);

        doSleep(AUTO_ADJUST_TIMEOUT / 2);

        stopGrid(2);

        doSleep(AUTO_ADJUST_TIMEOUT / 2);

        Set<Object> twoNodeLeftBaseline = ignite0.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        assertEquals(initBaseline, twoNodeLeftBaseline);

        assertTrue(waitForCondition(
            () -> isCurrentBaselineFromOneNode(ignite0),
            AUTO_ADJUST_TIMEOUT * 2
        ));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBaselineAutoAdjustSinceCoordinatorLeft() throws Exception {
        Ignite ignite0 = startGrids(3);

        ignite0.cluster().active(true);

        ignite0.cluster().baselineAutoAdjustTimeout(AUTO_ADJUST_TIMEOUT);

        Set<Object> initBaseline = ignite0.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        stopGrid(1);

        doSleep(AUTO_ADJUST_TIMEOUT / 2);

        stopGrid(0);

        doSleep(AUTO_ADJUST_TIMEOUT / 2);

        Ignite ignite2 = ignite(2);

        Set<Object> twoNodeLeftBaseline = ignite2.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        assertEquals(initBaseline, twoNodeLeftBaseline);

        assertTrue(waitForCondition(
            () -> isCurrentBaselineFromOneNode(ignite2),
            AUTO_ADJUST_TIMEOUT
        ));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBaselineAutoAdjustAfterNodeJoin() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        ignite0.cluster().baselineAutoAdjustTimeout(AUTO_ADJUST_TIMEOUT);

        assertTrue(isCurrentBaselineFromOneNode(ignite0));

        startGrid(1);

        assertTrue(isCurrentBaselineFromOneNode(ignite0));

        assertTrue(waitForCondition(
            () -> ignite0.cluster().currentBaselineTopology().size() == 2,
            AUTO_ADJUST_TIMEOUT * 2
        ));
    }

    /**
     * @param ignite0 Node to check.
     * @return {@code true} if current baseline consist from one node.
     */
    private boolean isCurrentBaselineFromOneNode(Ignite ignite0) {
        return ignite0.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .allMatch(((IgniteEx)ignite0).localNode().consistentId()::equals);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBaselineAutoAdjustDisabledAfterGridHasLostPart() throws Exception {
        AUTO_ADJUST_TIMEOUT = 0;

        Ignite ignite0 = startGrids(2);

        ignite0.cluster().active(true);

        Set<Object> initBaseline = ignite0.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        ignite0.cluster().baselineAutoAdjustTimeout(AUTO_ADJUST_TIMEOUT);

        IgniteCache<Object, Object> cache = ignite0.getOrCreateCache(new CacheConfiguration<>(TEST_NAME)
            .setBackups(0)
            .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE)
        );

        for (int j = 0; j < 500; j++)
            cache.put(j, "Value" + j);

        stopGrid(1);

        doSleep(3000);

        Set<Object> baselineAfterNodeLeft = ignite0.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        assertEquals(initBaseline, baselineAfterNodeLeft);
    }

    /**
     * @throws Exception if failed.
     */
    @Test(expected = BaselineAdjustForbiddenException.class)
    public void testBaselineAutoAdjustThrowExceptionWhenBaselineChangedManually() throws Exception {
        Ignite ignite0 = startGrids(2);

        ignite0.cluster().active(true);

        ignite0.cluster().baselineAutoAdjustTimeout(AUTO_ADJUST_TIMEOUT);

        Collection<BaselineNode> baselineNodes = ignite0.cluster().currentBaselineTopology();

        assertEquals(2, baselineNodes.size());

        stopGrid(1);

        ignite0.cluster().setBaselineTopology(Arrays.asList(((IgniteEx)ignite0).localNode()));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBaselineAutoAdjustTriggeredAfterFirstEventRegardlessInitBaseline() throws Exception {
        AUTO_ADJUST_TIMEOUT = 3000;

        Ignite ignite0 = startGrids(3);

        ignite0.cluster().active(true);

        ignite0.cluster().baselineAutoAdjustTimeout(AUTO_ADJUST_TIMEOUT);

        assertTrue(ignite0.cluster().isBaselineAutoAdjustEnabled());

        Set<Object> baselineNodes = ignite0.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        stopAllGrids();

        ignite0 = startGrids(2);

        ignite0.cluster().active(true);

        Set<Object> baselineNodesAfterRestart = ignite0.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        assertEquals(baselineNodes, baselineNodesAfterRestart);

        stopGrid(1);

        Ignite finalIgnite = ignite0;

        assertTrue(waitForCondition(
            () -> isCurrentBaselineFromOneNode(finalIgnite)
            ,
            AUTO_ADJUST_TIMEOUT * 2
        ));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBaselineAutoAdjustIgnoreClientNodes() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        startGrid(1);

        ignite0.cluster().active(true);

        ignite0.cluster().baselineAutoAdjustTimeout(AUTO_ADJUST_TIMEOUT);

        assertTrue(ignite0.cluster().isBaselineAutoAdjustEnabled());

        stopGrid(1);

        doSleep(AUTO_ADJUST_TIMEOUT / 2);

        IgniteEx igniteClient = startGrid(getConfiguration(getTestIgniteInstanceName(2)).setClientMode(true));

        doSleep(AUTO_ADJUST_TIMEOUT / 2);

        igniteClient.close();

        assertTrue(isCurrentBaselineFromOneNode(ignite0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBaselineAutoAdjustDisableByDefaultBecauseNotNewCluster() throws Exception {
        System.setProperty(IGNITE_BASELINE_AUTO_ADJUST_ENABLED, "false");
        try {
            Ignite ignite0 = startGrids(2);

            ignite0.cluster().active(true);

            ignite0.cluster().baselineAutoAdjustTimeout(0);

            stopAllGrids();
        }
        finally {
            System.clearProperty(IGNITE_BASELINE_AUTO_ADJUST_ENABLED);
        }

        Ignite ignite0 = startGrids(2);

        ignite0.cluster().active(true);

        Set<Object> initBaseline = ignite0.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        stopGrid(1);

        assertFalse(waitForCondition(
            () -> !initBaseline.equals(
                ignite0.cluster().currentBaselineTopology().stream()
                    .map(BaselineNode::consistentId)
                    .collect(Collectors.toSet())
            ),
            5_000
        ));
    }
}
