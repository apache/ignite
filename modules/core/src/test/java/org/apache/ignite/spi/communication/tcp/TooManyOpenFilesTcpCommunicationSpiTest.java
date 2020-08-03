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

package org.apache.ignite.spi.communication.tcp;

import java.io.IOException;
import java.net.SocketException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test class for error "Too many open files" when in {@link TcpCommunicationSpi}.
 */
public class TooManyOpenFilesTcpCommunicationSpiTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setCommunicationSpi(new TooManyOpenFilesTcpCommunicationSpi())
            .setConsistentId(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                    .setAtomicityMode(TRANSACTIONAL)
                    .setBackups(1)
                    .setCacheMode(PARTITIONED)
            );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Test checks that node will fail in case of error "Too many open files"
     * in {@link TcpCommunicationSpi}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTooManyOpenFilesErr() throws Exception {
        IgniteEx crd = startGrids(3);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteEx stopNode = grid(2);

        TooManyOpenFilesTcpCommunicationSpi stopNodeSpi = (TooManyOpenFilesTcpCommunicationSpi)
            stopNode.context().config().getCommunicationSpi();

        IgniteEx txNode = grid(1);

        try (Transaction tx = txNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 60_000, 4)) {
            IgniteCache<Object, Object> cache = txNode.cache(DEFAULT_CACHE_NAME);

            cache.put(0, 1);

            stopNodeSpi.throwException.set(true);
            stopNodeSpi.closeConnections(txNode.localNode().id());

            cache.put(1, 2);
            cache.put(2, 3);
            cache.put(3, 4);

            // hungs here.
            tx.commit();
        }
        catch (ClusterTopologyException e) {
            log.error("Error wait commit", e);
        }

        assertTrue(waitForCondition(((IgniteKernal)stopNode)::isStopping, 60_000));
    }

    /**
     * Class for emulating "Too many open files" error in
     * {@link TcpCommunicationSpi}.
     */
    private static class TooManyOpenFilesTcpCommunicationSpi extends TcpCommunicationSpi {
        /** Flag for throwing an exception "Too many open files". */
        private final AtomicBoolean throwException = new AtomicBoolean();

        /** {@inheritDoc} */
        @Override protected SocketChannel openSocketChannel() throws IOException {
            if (throwException.get())
                throw new SocketException("Too many open files");

            return super.openSocketChannel();
        }
    }
}
