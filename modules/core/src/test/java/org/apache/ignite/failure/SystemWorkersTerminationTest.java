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

package org.apache.ignite.failure;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThread;

/**
 * Tests system critical workers termination.
 */
public class SystemWorkersTerminationTest extends GridCommonAbstractTest {
    /** */
    private static volatile String failureHndThreadName;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureHandler(new TestFailureHandler());

        DataRegionConfiguration drCfg = new DataRegionConfiguration();
        drCfg.setPersistenceEnabled(true);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.setDefaultDataRegionConfiguration(drCfg);
        dsCfg.setWalCompactionEnabled(true);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        deleteWorkFiles();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        deleteWorkFiles();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSyntheticWorkerTermination() throws Exception {
        IgniteEx ignite = grid(0);

        WorkersRegistry registry = ignite.context().workersRegistry();

        long fdTimeout = ignite.configuration().getFailureDetectionTimeout();

        GridWorker worker = new GridWorker(ignite.name(), "test-worker", log, registry) {
            @Override protected void body() throws InterruptedException {
                Thread.sleep(fdTimeout / 2);
            }
        };

        IgniteThread thread = new IgniteThread(worker);

        failureHndThreadName = null;

        thread.start();

        thread.join();

        assertTrue(GridTestUtils.waitForCondition(() -> thread.getName().equals(failureHndThreadName), fdTimeout * 2));
    }

    /**
     * @throws Exception If failed.
     */
    private void deleteWorkFiles() throws Exception {
        cleanPersistenceDir();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "snapshot", false));
    }

    /**
     * Test failure handler.
     */
    private class TestFailureHandler extends AbstractFailureHandler {
        /** {@inheritDoc} */
        @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
            if (failureCtx.type() == FailureType.SYSTEM_WORKER_TERMINATION)
                failureHndThreadName = Thread.currentThread().getName();

            return false;
        }
    }
}
