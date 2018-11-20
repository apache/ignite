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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThread;

/**
 * Tests system critical workers termination.
 */
public class SystemWorkersTerminationTest extends GridCommonAbstractTest {
    /** Handler latch. */
    private static volatile CountDownLatch hndLatch;

    /** */
    private static final long FAILURE_DETECTION_TIMEOUT = 5_000;

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

        cfg.setFailureDetectionTimeout(FAILURE_DETECTION_TIMEOUT);

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
    public void testTermination() throws Exception {
        Ignite ignite = ignite(0);

        ignite.cluster().active(true);

        WorkersRegistry registry = ((IgniteKernal)ignite).context().workersRegistry();

        Collection<String> threadNames = new ArrayList<>(registry.names());

        int cnt = 0;

        for (String threadName : threadNames) {
            log.info("Worker termination: " + threadName);

            hndLatch = new CountDownLatch(1);

            GridWorker w = registry.worker(threadName);

            Thread t = w.runner();

            t.interrupt();

            assertTrue(hndLatch.await(3, TimeUnit.SECONDS));

            log.info("Worker is terminated: " + threadName);

            cnt++;
        }

        assertEquals(threadNames.size(), cnt);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSyntheticWorkerTermination() throws Exception {
        hndLatch = new CountDownLatch(1);

        IgniteEx ignite = grid(0);

        GridWorker worker = new GridWorker(ignite.name(), "test-worker", log) {
            @Override protected void body() throws InterruptedException {
                Thread.sleep(ignite.configuration().getFailureDetectionTimeout() / 2);
            }
        };

        new IgniteThread(worker).start();

        while (worker.runner() == null)
            Thread.sleep(10);

        ignite.context().workersRegistry().register(worker);

        worker.runner().join();

        assertTrue(hndLatch.await(ignite.configuration().getFailureDetectionTimeout() * 2, TimeUnit.MILLISECONDS));
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
            hndLatch.countDown();

            return false;
        }
    }
}
