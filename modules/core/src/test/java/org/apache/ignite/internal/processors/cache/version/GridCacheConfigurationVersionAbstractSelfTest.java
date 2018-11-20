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
package org.apache.ignite.internal.processors.cache.version;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.processors.cache.version.GridCacheConfigurationChangeAction.DESTROY;

/**
 *
 */
public abstract class GridCacheConfigurationVersionAbstractSelfTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataRegionConfiguration drCfg = new DataRegionConfiguration().setPersistenceEnabled(true);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(drCfg);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    public void testSingleNode() throws Exception {
        testSameVersionOnNodes(1, 0, 0, false, null);
    }

    /** */
    public void testTwoNodes0() throws Exception {
        testSameVersionOnNodes(2, 0, 0, false, null);
    }

    /** */
    public void testTwoNodes1() throws Exception {
        testSameVersionOnNodes(2, 1, 0, false, null);
    }

    /** */
    public void testTwoNodesWithStopSecond1() throws Exception {
        testSameVersionOnNodes(2, 0, 1, false, null);
    }

    /** */
    public void testTwoNodesWithStopSecond2() throws Exception {
        testSameVersionOnNodes(2, 0, 2, false, null);
    }

    /** */
    public void testTwoNodesWithStopSecond1RestartNatural() throws Exception {
        testSameVersionOnNodes(2, 0, 1, true, Comparator.naturalOrder());
    }

    /** */
    public void testTwoNodesWithStopSecond1RestartReverse() throws Exception {
        testSameVersionOnNodes(2, 0, 1, true, Comparator.reverseOrder());
    }

    /** */
    public void testTwoNodesWithStopSecond2RestartNatural() throws Exception {
        testSameVersionOnNodes(2, 0, 2, true, Comparator.naturalOrder());
    }

    /** */
    public void testTwoNodesWithStopSecond2RestartReverse() throws Exception {
        testSameVersionOnNodes(2, 0, 2, true, Comparator.reverseOrder());
    }

    /** */
    protected final void testSameVersionOnNodes(
        int nodesCnt,
        int performNodeId,
        int skipRounds,
        boolean stopGrid,
        Comparator<Integer> startOrder
    ) throws Exception {
        assert nodesCnt > 0 : nodesCnt;
        assert performNodeId >= 0 : performNodeId;
        assert performNodeId < nodesCnt;
        assert skipRounds >= 0 : skipRounds;
        assert skipRounds == 0 || nodesCnt > 1;
        assert skipRounds == 0 || performNodeId < nodesCnt / 2;
        assert !stopGrid || startOrder != null;

        int verCntr = 0;

        startGrids(nodesCnt);

        IgniteEx ignite = grid(performNodeId);

        ignite.cluster().active(true);

        performActionOnStartTestAfterClusterActivate(ignite);

        verCntr = performActionsOnCache(0, nodesCnt, verCntr, ignite);

        if (skipRounds > 0) {
            for (int i = nodesCnt / 2; i < nodesCnt; i++)
                stopGrid(i);

            for (int i = 0; i < skipRounds; i++)
                verCntr = performActionsOnCache(0, nodesCnt / 2, verCntr, ignite);

            if (stopGrid) {
                for (int i = 0; i < nodesCnt / 2; i++)
                    stopGrid(i);

                List<Integer> order = IntStream.range(0, nodesCnt)
                    .boxed()
                    .sorted(startOrder)
                    .collect(Collectors.toList());

                for (int i : order)
                    startGrid(i);

                ignite = grid(order.get(0));

                ignite.cluster().active(true);
            }
            else {
                for (int i = nodesCnt / 2; i < nodesCnt; i++)
                    startGrid(i);
            }
        }

        verCntr = performActionsOnCache(0, nodesCnt, verCntr, ignite);
    }

    /** */
    protected abstract int performActionsOnCache(
        int firstNodeId,
        int lastNodeId,
        int ver,
        IgniteEx ignite
    ) throws Exception;

    /** */
    protected void performActionOnStartTestAfterClusterActivate(IgniteEx ignite) throws Exception {
        // No-op.
    }

    /** */
    protected final void checkCacheVersion(
        @NotNull IgniteEx ignite,
        String cacheName,
        int verId,
        GridCacheConfigurationChangeAction act
    ) {
        GridCacheConfigurationVersion ver = ignite.context().cache().cacheVersion(cacheName);

        assertEquals(verId, ver.id());
        assertEquals(act, ver.lastAction());

        DynamicCacheDescriptor desc = ignite.context().cache().cacheDescriptor(cacheName);

        if (act == DESTROY)
            assertNull(desc);
    }

    /** */
    protected void awaitCacheVersion(
        int firstNodeId,
        int lastNodeId,
        String cacheName,
        int verId,
        long timeout
    ) throws TimeoutException {
        long leftTime = timeout;
        while (leftTime > 0) {
            long sleepTime = Math.min(50L, leftTime);

            doSleep(sleepTime);

            boolean verReached = true;

            for (int i = firstNodeId; i < lastNodeId; i++) {
                GridCacheConfigurationVersion ver = grid(i).context().cache().cacheVersion(cacheName);

                if (ver.id() != verId) {
                    verReached = false;

                    break;
                }
            }

            if (verReached)
                return;

            leftTime -= sleepTime;
        }

        throw new TimeoutException("Version wasn't reached! cacheName: " + cacheName + " version: " + verId);
    }
}