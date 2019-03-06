/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxFinishResponse;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test cases for preload tests.
 */
public class GridCacheReplicatedSynchronousCommitTest extends GridCommonAbstractTest {
    /** */
    private static final int ADDITION_CACHE_NUMBER = 2;

    /** */
    private static final String NO_COMMIT = "no_commit";

    /** */
    private final Collection<TestCommunicationSpi> commSpis = new ConcurrentLinkedDeque<>();

    /**
     *
     */
    public GridCacheReplicatedSynchronousCommitTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(CacheMode.REPLICATED);

        cc.setWriteSynchronizationMode(FULL_SYNC);

        c.setCacheConfiguration(cc);

        TestCommunicationSpi commSpi = new TestCommunicationSpi(igniteInstanceName.equals(NO_COMMIT));

        c.setCommunicationSpi(commSpi);

        commSpis.add(commSpi);

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
    @Test
    public void testSynchronousCommit() throws Exception {
        try {
            Ignite firstIgnite = startGrid("1");

            IgniteCache<Integer, String> firstCache = firstIgnite.cache(DEFAULT_CACHE_NAME);

            for (int i = 0; i < ADDITION_CACHE_NUMBER; i++)
                startGrid(String.valueOf(i + 2));

            firstCache.put(1, "val1");

            int cnt = 0;

            for (TestCommunicationSpi commSpi : commSpis)
                cnt += commSpi.messagesCount();

            assertEquals(ADDITION_CACHE_NUMBER, cnt);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testSynchronousCommitNodeLeave() throws Exception {
        try {
            Ignite ignite1 = startGrid("1");

            startGrid(NO_COMMIT);

            Ignite ignite3 = startGrid("3");

            IgniteCache<Integer, String> cache1 = ignite1.cache(DEFAULT_CACHE_NAME);
            IgniteCache<Integer, String> cache3 = ignite3.cache(DEFAULT_CACHE_NAME);

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
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            Object obj = ((GridIoMessage)msg).message();

            if (obj instanceof GridDistributedTxFinishResponse) {
                msgCnt.incrementAndGet();

                if (noCommit)
                    return;
            }

            super.sendMessage(node, msg, ackClosure);
        }
    }
}
