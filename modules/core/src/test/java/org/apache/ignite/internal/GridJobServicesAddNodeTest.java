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

package org.apache.ignite.internal;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.service.DummyService;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

/**
 * Tests multiple parallel jobs execution, accessing services(), while starting new nodes.
 */
@GridCommonTest(group = "Kernal Self")
public class GridJobServicesAddNodeTest extends GridCommonAbstractTest {
    /** */
    private static final int LOG_MOD = 100;

    /** */
    private static final int MAX_ADD_NODES = 64;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(1);
        startGrid(2);

        assertEquals(2, grid(1).cluster().nodes().size());
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        c.setCommunicationSpi(commSpi);

        return c;
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testServiceDescriptorsJob() throws Exception {
        final int tasks = 5000;
        final int threads = 10;

        final Ignite ignite1 = grid(1);
        final CountDownLatch latch = new CountDownLatch(tasks);
        final AtomicInteger jobsCnt = new AtomicInteger();
        final AtomicInteger resCnt = new AtomicInteger();

        ignite1.services().deployClusterSingleton("jobsSvc", new DummyService());

        GridTestUtils.runMultiThreadedAsync(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                while (true) {
                    int cnt = jobsCnt.incrementAndGet();

                    if (cnt > 5000)
                        break;

                    IgniteCallable<Boolean> job;

                    job = new ServiceDescriptorsJob();

                    IgniteFuture<Boolean> fut = ignite1.compute().callAsync(job);

                    if (cnt % LOG_MOD == 0)
                        X.println("Submitted jobs: " + cnt);

                    fut.listen(new CIX1<IgniteFuture<Boolean>>() {
                        @Override public void applyx(IgniteFuture<Boolean> f) {
                            try {
                                assert f.get();

                                long cnt = resCnt.incrementAndGet();

                                if (cnt % LOG_MOD == 0)
                                    X.println("Results count: " + cnt);
                            }
                            finally {
                                latch.countDown();
                            }
                        }
                    });

                    IgniteUtils.sleep(5);
                }
            }
        }, threads, "TEST-THREAD");

        int additionalNodesStarted = 0;
        while (!latch.await(threads, TimeUnit.MILLISECONDS)) {
            if (additionalNodesStarted++ <= MAX_ADD_NODES) {
                startGrid(2 + additionalNodesStarted);
            }
        }

        assertEquals("Jobs cnt != Results cnt", jobsCnt.get() - threads, resCnt.get());
    }

    /**
     * Test service enumerating job.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class ServiceDescriptorsJob implements IgniteCallable<Boolean> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public Boolean call() throws Exception {
            try {
                return ignite.services().serviceDescriptors().iterator().hasNext();
            } catch (Exception e) {
                e.printStackTrace();

                return false;
            } finally {
                Thread.sleep(10);
            }
        }
    }
}
