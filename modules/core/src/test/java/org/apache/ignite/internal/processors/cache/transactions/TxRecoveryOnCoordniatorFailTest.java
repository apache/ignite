/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests failure handler is not triggered after client node and coordinator left cluster.
 */
public class TxRecoveryOnCoordniatorFailTest extends GridCommonAbstractTest {
    /**  */
    private AtomicReference<Throwable> err;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setSystemThreadPoolSize(1)
            .setFailureHandler(new AbstractFailureHandler() {
                @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                    err.compareAndSet(null, failureCtx.error());

                    return false;
                }
            });
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        err = new AtomicReference<>();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Checks for the absence of critical failures caused by tx recovery after client and coordinator left the cluster.
     * <p>Test scenario:</p>
     * <ul>
     *  <li>Start 2 server nodes and client node.</li>
     *  <li>Execute long runing task in the single threaded system pool on the second server node. It delays execution of tx recovery.</li>
     *  <li>Stop client and coordinator nodes.</li>
     *  <li>Check triggering of failure handler.</li>
     * </ul>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCoordinatorLeftCluster() throws Exception {
        startGrids(2);

        Ignite client = startClientGrid(2);

        Executor sysPool = grid(1).context().pools().poolForPolicy(GridIoPolicy.SYSTEM_POOL);

        sysPool.execute(new Runnable() {
            @Override public void run() {
                try {
                    waitForTopology(1);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                catch (Exception ignored) {
                }
            }
        });

        client.close();

        grid(0).close();

        // Make sure that tx recovery is already executed in the system thread pool.
        CountDownLatch latch = new CountDownLatch(1);
        sysPool.execute(latch::countDown);

        latch.await();

        Throwable error = err.get();

        if (error != null)
            Assert.fail("Critical failure occurred '" + error + "'");
    }
}
