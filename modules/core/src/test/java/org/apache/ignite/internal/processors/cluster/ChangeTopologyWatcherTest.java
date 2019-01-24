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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

@GridCommonTest(group = "Kernal Self")
@RunWith(JUnit4.class)
public class ChangeTopologyWatcherTest extends GridCommonAbstractTest {

    public static final String TEST_NAME = "TEST_NAME";
    public static int AUTO_ADJUST_TIMEOUT = 5000;

    @Before
    public void before() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        AUTO_ADJUST_TIMEOUT = 5000;
    }

    @After
    public void after() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);
        cfg.setInitBaselineAutoAdjustTimeout(AUTO_ADJUST_TIMEOUT);
        cfg.setInitBaselineAutoAdjustMaxTimeout(12000);
        cfg.setInitBaselineAutoAdjustEnabled(true);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize(500L * 1024 * 1024);

        cfg.setDataStorageConfiguration(storageCfg);

        return cfg;
    }

    @Test
    public void testBaselineAutoAdjustAfterNodeLeft() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        ignite0.cluster().active(true);

        Set<Object> initBaseline = ignite0.cluster().currentBaselineTopology().stream().map(BaselineNode::consistentId).collect(Collectors.toSet());

        stopGrid(1);

        Set<Object> nodeLeftBaseline = ignite0.cluster().currentBaselineTopology().stream().map(BaselineNode::consistentId).collect(Collectors.toSet());

        assertEquals(initBaseline, nodeLeftBaseline);

        assertTrue(waitForCondition(
            () -> isCurrentBaselineFromOneNode(ignite0),
            AUTO_ADJUST_TIMEOUT * 2
        ));
    }

    @Test
    public void testBaselineAutoAdjustSinceSecondNodeLeft() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);
        IgniteEx ignite2 = startGrid(2);

        ignite0.cluster().active(true);

        Set<Object> initBaseline = ignite0.cluster().currentBaselineTopology().stream().map(BaselineNode::consistentId).collect(Collectors.toSet());

        stopGrid(1);

        long stopFirstNodeTime = System.currentTimeMillis();

        doSleep(AUTO_ADJUST_TIMEOUT / 2);

        stopGrid(2);

        assertTrue(waitForCondition(
            () -> AUTO_ADJUST_TIMEOUT * 1.4 < System.currentTimeMillis() - stopFirstNodeTime,
            AUTO_ADJUST_TIMEOUT
        ));

        Set<Object> twoNodeLeftBaseline = ignite0.cluster().currentBaselineTopology().stream().map(BaselineNode::consistentId).collect(Collectors.toSet());

        assertEquals(initBaseline, twoNodeLeftBaseline);

        assertTrue(waitForCondition(
            () -> isCurrentBaselineFromOneNode(ignite0),
            AUTO_ADJUST_TIMEOUT * 2
        ));
    }

    @Test
    public void testBaselineAutoAdjustAfterNodeJoin() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        assertTrue(isCurrentBaselineFromOneNode(ignite0));

        IgniteEx ignite1 = startGrid(1);

        assertTrue(isCurrentBaselineFromOneNode(ignite0));

        assertTrue(waitForCondition(
            () -> ignite0.cluster().currentBaselineTopology().size() == 2
                && ignite0.cluster().currentBaselineTopology().stream().map(BaselineNode::consistentId).anyMatch(ignite1.localNode().consistentId()::equals)
            ,
            AUTO_ADJUST_TIMEOUT * 2
        ));
    }

    private boolean isCurrentBaselineFromOneNode(IgniteEx ignite0) {
        return ignite0.cluster().currentBaselineTopology().stream().map(BaselineNode::consistentId).allMatch(ignite0.localNode().consistentId()::equals);
    }

//    @Test
//    public void testBaselineAutoAdjustAfterNodeLeft2() throws Exception {
//        AUTO_ADJUST_TIMEOUT = 0;
//
//        IgniteEx ignite0 = startGrid(0);
//        IgniteEx ignite1 = startGrid(1);
//
//        ignite0.cluster().active(true);
//
//        ignite0.cluster().active(false);
//
//        IgniteEx ignite2 = startGrid(2);
//
//        ignite0.cluster().active(true);
//
//        assertEquals(2, ignite0.cluster().currentBaselineTopology().size());
//        assertTrue(ignite0.cluster().currentBaselineTopology().stream().map(BaselineNode::consistentId).allMatch(consId ->
//            ignite0.localNode().consistentId().equals(consId) || ignite1.localNode().consistentId().equals(consId)));
//
//        stopGrid(1);
//
//        assertTrue(waitForCondition(
//            () -> isCurrentBaselineFromOneNode(ignite0)
//            ,
//            5000
//        ));
//
//        stopGrid(2);
//
//        assertFalse(waitForCondition(
//            () -> !isCurrentBaselineFromOneNode(ignite0)
//            ,
//            3000
//        ));
//    }

    @Test
    public void testBaselineAutoAdjustDisabledAfterGridHasLostPart() throws Exception {
        AUTO_ADJUST_TIMEOUT = 0;
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        ignite0.cluster().active(true);

        IgniteCache<Object, Object> cache = ignite0.getOrCreateCache(new CacheConfiguration<>(TEST_NAME)
            .setBackups(0)
            .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE)
        );

        for (int j = 0; j < 500; j++)
            cache.put(j, "Value" + j);

        stopGrid(1);

        assertFalse(waitForCondition(
            () -> isCurrentBaselineFromOneNode(ignite0)
            ,
            5000
        ));

        assertFalse(ignite0.cluster().baselineConfiguration().isBaselineAutoAdjustEnabled());
    }

    @Test
    public void testBaselineAutoAdjustDisabledAfterBaselineChangedManually() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);
        IgniteEx ignite2 = startGrid(2);

        ignite0.cluster().active(true);

        Collection<BaselineNode> baselineNodes = ignite0.cluster().currentBaselineTopology();

        assertEquals(3, baselineNodes.size());

        List<ClusterNode> nodes = Arrays.asList(ignite0.localNode(), ignite1.localNode());

        stopGrid(1);
        stopGrid(2);

        ignite0.cluster().setBaselineTopology(nodes);

        assertEquals(2, ignite0.cluster().currentBaselineTopology().size());

        assertFalse(waitForCondition(
            () -> isCurrentBaselineFromOneNode(ignite0)
            ,
            AUTO_ADJUST_TIMEOUT * 2
        ));

        assertFalse(ignite0.cluster().baselineConfiguration().isBaselineAutoAdjustEnabled());
    }

    @Test
    public void testBaselineAutoAdjustNotDisabledAfterBaselineChangedManually() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        ignite0.cluster().active(true);

        Collection<BaselineNode> baselineNodes = ignite0.cluster().currentBaselineTopology();

        assertEquals(2, baselineNodes.size());

        stopGrid(1);

        ignite0.cluster().setBaselineTopology(Arrays.asList(ignite0.localNode()));

        assertEquals(1, ignite0.cluster().currentBaselineTopology().size());

        assertTrue(waitForCondition(
            () -> isCurrentBaselineFromOneNode(ignite0)
            ,
            AUTO_ADJUST_TIMEOUT
        ));

        assertTrue(ignite0.cluster().baselineConfiguration().isBaselineAutoAdjustEnabled());
    }

    @Test
    public void testBaselineAutoAdjustDisabledBaselineNotEqualGridActivation() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        ignite0.cluster().active(true);

        assertTrue(ignite0.cluster().baselineConfiguration().isBaselineAutoAdjustEnabled());

        stopAllGrids();

        ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        assertFalse(ignite0.cluster().baselineConfiguration().isBaselineAutoAdjustEnabled());
    }

}