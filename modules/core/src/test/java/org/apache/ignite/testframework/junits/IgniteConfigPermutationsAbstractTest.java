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

package org.apache.ignite.testframework.junits;

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.TestsConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Common abstract test for Ignite tests based on configurations permutations.
 */
public abstract class IgniteConfigPermutationsAbstractTest extends GridCommonAbstractTest {
    /** */
    private static final File workDir = new File(U.getIgniteHome() + File.separator + "workOfConfigPermutationsTests");

    /** */
    protected TestsConfiguration testsCfg;

    /**
     * @param testsCfg Tests configuration.
     */
    public void setTestsConfiguration(TestsConfiguration testsCfg) {
        assert this.testsCfg == null: "Test config must be set only once [oldTestCfg=" + this.testsCfg
            + ", newTestCfg=" + testsCfg + "]";

        this.testsCfg = testsCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        assert testsCfg != null;

        FileUtils.deleteDirectory(workDir);

        info("Ignite's 'work' directory has been cleaned.");

        if (Ignition.allGrids().size() != testsCfg.gridCount()) {
            info("All nodes will be stopped, new " + testsCfg.gridCount() + " nodes will be started.");

            Ignition.stopAll(true);

            startGrids(testsCfg.gridCount());

            for (int i = 0; i < testsCfg.gridCount(); i++)
                info("Grid " + i + ": " + grid(i).localNode().id());
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        if (testsCfg.isStopNodes()) {
            info("Stopping all grids...");

            stopAllGrids();

            FileUtils.deleteDirectory(workDir);

            info("Ignite's 'work' directory has been cleaned.");

            memoryUsage();

            System.gc();

            memoryUsage();
        }
    }

    /**
     * Prints memory usage.
     */
    private void memoryUsage() {
        int mb = 1024*1024;

        Runtime runtime = Runtime.getRuntime();

        info("##### Heap utilization statistics [MB] #####");
        info("Used Memory  (mb): " + (runtime.totalMemory() - runtime.freeMemory()) / mb);
        info("Free Memory  (mb): " + runtime.freeMemory() / mb);
        info("Total Memory (mb): " + runtime.totalMemory() / mb);
        info("Max Memory   (mb): " + runtime.maxMemory() / mb);
    }

    /** {@inheritDoc} */
    @Override protected String testClassDescription() {
        return super.testClassDescription() + '-' + testsCfg.description() + '-' + testsCfg.gridCount() + "-node(s)";
    }

    /** {@inheritDoc} */
    @Override protected String testDescription() {
        return super.testDescription() + '-' + testsCfg.description() + '-' + testsCfg.gridCount() + "-node(s)";
    }

    /** {@inheritDoc} */
    @Override protected final IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        IgniteConfiguration resCfg = testsCfg.configurationFactory().getConfiguration(gridName, cfg);

        resCfg.setWorkDirectory(workDir.getAbsolutePath());

        return resCfg;
    }

    /** {@inheritDoc} */
    protected final int gridCount() {
        return testsCfg.gridCount();
    }

    /**
     * @return Count of clients.
     */
    protected int clientsCount() {
        int cnt = 0;

        for (int i = 0; i < gridCount(); i++) {
             if (grid(i).configuration().isClientMode())
                 cnt++;
        }

        return cnt;
    }
}
