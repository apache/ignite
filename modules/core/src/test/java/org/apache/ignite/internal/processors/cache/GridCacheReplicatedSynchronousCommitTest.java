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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Test cases for preload tests.
 */
public class GridCacheReplicatedSynchronousCommitTest extends GridCommonAbstractTest {
    /** */
    private static final int ADDITION_CACHE_NUMBER = 2;

    /** */
    private static final String NO_COMMIT = "no_commit";

    /** */
    private final Collection<TestCommunicationSpi> commSpis = new ConcurrentLinkedDeque8<>();

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     *
     */
    public GridCacheReplicatedSynchronousCommitTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(CacheMode.REPLICATED);

        cc.setWriteSynchronizationMode(FULL_SYNC);

        c.setCacheConfiguration(cc);

        TestCommunicationSpi commSpi = new TestCommunicationSpi(gridName.equals(NO_COMMIT));

        c.setCommunicationSpi(commSpi);

        commSpis.add(commSpi);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        commSpis.clear();
    }

    /**
     * @throws Exception If test failed.
     */
    public void testSynchronousCommit() throws Exception {
        try {
            Ignite firstIgnite = startGrid("1");

            IgniteCache<Integer, String> firstCache = firstIgnite.cache(null);

            for (int i = 0; i < ADDITION_CACHE_NUMBER; i++)
                startGrid(String.valueOf(i + 2));

            firstCache.put(1, "val1");

            int cnt = 0;

            for (TestCommunicationSpi commSpi : commSpis)
                cnt += commSpi.messagesCount();

            assert cnt == ADDITION_CACHE_NUMBER;
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testSynchronousCommitNodeLeave() throws Exception {
        try {
            Ignite ignite1 = startGrid("1");

            startGrid(NO_COMMIT);

            Ignite ignite3 = startGrid("3");

            IgniteCache<Integer, String> cache1 = ignite1.cache(null);
            IgniteCache<Integer, String> cache3 = ignite3.cache(null);

            IgniteInternalFuture<?> fut = multithreadedAsync(
                new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        Thread.sleep(1000);

                        stopGrid(NO_COMMIT);

                        return null;
                    }
                },
                1);

            cache1.put(1, "val1");

            assert cache3.get(1) != null;

            fut.get();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private final AtomicInteger msgCnt = new AtomicInteger();

        /** */
        private boolean noCommit;

        /**
         * @param noCommit Send Commit or not.
         */
        private TestCommunicationSpi(boolean noCommit) {
            this.noCommit = noCommit;
        }

        /**
         * @return Number of transaction finish messages that was sent.
         */
        public int messagesCount() {
            return msgCnt.get();
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg)
            throws IgniteSpiException {
            Object obj = ((GridIoMessage)msg).message();

            if (obj instanceof GridDistributedTxFinishResponse) {
                msgCnt.incrementAndGet();

                if (noCommit)
                    return;
            }

            super.sendMessage(node, msg);
        }
    }
}
