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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test cases for partitioned cache {@link GridDhtPreloader preloader}.
 */
public class GridCacheDhtPreloadPerformanceTest extends GridCommonAbstractTest {
    /** */
    private static final int THREAD_CNT = 30;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(
            CacheMode.PARTITIONED);
        cc.setWriteSynchronizationMode(
            CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setRebalanceMode(
            CacheRebalanceMode.SYNC);
        cc.setAffinity(new RendezvousAffinityFunction(false, 1300));
        cc.setBackups(2);

        CacheConfiguration cc1 = defaultCacheConfiguration();

        cc1.setName("cc1");
        cc1.setCacheMode(
            CacheMode.PARTITIONED);
        cc1.setWriteSynchronizationMode(
            CacheWriteSynchronizationMode.FULL_SYNC);
        cc1.setRebalanceMode(
            CacheRebalanceMode.SYNC);
        cc1.setAffinity(
            new RendezvousAffinityFunction(
                false,
                1300));
        cc1.setBackups(2);

        c.setSystemThreadPoolSize(2);
        c.setPublicThreadPoolSize(2);
        c.setManagementThreadPoolSize(1);
        c.setUtilityCachePoolSize(2);
        c.setPeerClassLoadingThreadPoolSize(1);

        c.setCacheConfiguration(cc, cc1);

        TcpCommunicationSpi comm = new TcpCommunicationSpi();

        comm.setSharedMemoryPort(-1);

        c.setCommunicationSpi(comm);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentStartPerformance() throws Exception {
//
//        for (int i = 0; i < 10; i++) {
//            try {
//                startGrid(1);
//                startGrid(2);
//                startGrid(3);
//            }
//            finally {
//                G.stopAll(true);
//            }
//        }

        multithreaded(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    long start = U.currentTimeMillis();

                    Ignite grid = startGrid(Thread.currentThread().getName());

                    System.out.println(
                        ">>> Time to start: " + (U.currentTimeMillis() - start) +
                            ", topSize=" + grid.cluster().nodes().size());

                    return null;
                }
            },
            THREAD_CNT);
    }
}
