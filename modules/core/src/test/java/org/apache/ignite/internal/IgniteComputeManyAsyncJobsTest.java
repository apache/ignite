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

import java.util.LinkedList;
import java.util.Queue;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Submits many async tasks that do cache operations.
 */
public class IgniteComputeManyAsyncJobsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);

        // Commented code resolves hang.
//        if (gridName.endsWith("1"))
//            ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setMessageQueueLimit(0);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        // Not necessary, but hangs faster.
        return true;
    }

    /**
     * @throws Exception If failed.
     */
    public void testManyJobs() throws Exception {
        final IgniteEx ignite0 = startGrid(0);
        startGrid(1);

        final IgniteCompute compute = ignite0.compute(ignite0.cluster().forRemotes()).withAsync();

        Queue<IgniteFuture> futs = new LinkedList<>();

        for (int i = 0; i < 20_000; i++) {
            compute.run(new TestJob(i));

            futs.add(compute.future());
        }

        IgniteFuture fut;

        do {
            fut = futs.poll();

            if (fut != null)
                fut.get();
        } while (fut != null);
    }

    /**
     *
     */
    private static class TestJob implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0L;
        /** */
        @IgniteInstanceResource
        private transient Ignite ignite;

        /** */
        private final int i;

        /**
         * @param i Integer.
         */
        private TestJob(final int i) {
            this.i = i;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            final IgniteCache<Integer, String> cache = ignite.getOrCreateCache("cache");

            System.out.println(">> Executed " + i);

            cache.get(i);
            cache.put(i, String.valueOf(i));
        }
    }
}
